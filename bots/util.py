from abc import abstractmethod
from enum import Enum
import logging
import sys
import threading
import time

from web3 import eth

from constants.addresses import *
from data_access.graphs import (
    BeanSqlClient, BeanstalkSqlClient, LAST_PEG_CROSS_FIELD, PRICE_FIELD)
from data_access import eth_chain

# Strongly encourage Python 3.8+.
# If not 3.8+ uncaught exceptions on threads will not be logged.
MIN_PYTHON = (3, 8)
if sys.version_info < MIN_PYTHON:
    logging.critical(
        "Python %s.%s or later is required for proper exception logging.\n" % MIN_PYTHON)
LOGGING_FORMAT_STR_SUFFIX = '%(levelname)s : %(asctime)s : %(message)s'
LOGGING_FORMATTER = logging.Formatter(LOGGING_FORMAT_STR_SUFFIX)

TIMESTAMP_KEY = 'timestamp'
# The duration of a season. Assumes that seasons align with Unix epoch.
SEASON_DURATION = 3600  # seconds
# For all check periods there is a built in assumption that we will update at least once per
# Ethereum block (~13.5 seconds).
# How long to wait between peg checks.
PEG_CHECK_PERIOD = 12  # seconds
# How long to wait between price updates.
PRICE_CHECK_PERIOD = 12  # seconds
# How long to wait between checks for a sunrise when we expect a new season to begin.
SUNRISE_CHECK_PERIOD = 12  # seconds
# Rate at which to check chain for new Uniswap V2 pool interactions.
POOL_CHECK_RATE = 12  # seconds
# Rate at which to check for events on the Beanstalk contract.
BEANSTALK_CHECK_RATE = 12  # seconds
# Bytes in 50 megabytes.
FIVE_HUNDRED_MEGABYTES = 500**6
# Initial time to wait before reseting dead monitor.
RESET_MONITOR_DELAY_INIT = 5 # seconds


class PegCrossType(Enum):
    NO_CROSS = 0
    CROSS_ABOVE = 1
    CROSS_BELOW = 2


class Monitor():
    """Base class for monitors. Do not use directly.

    Args:
        name: simple human readable name string to use for logging.
        message_function: fun(str) style function to send application messages.
        query_rate: int representing rate monitored data should be queried (in seconds).
        prod: bool indicating if this is a production instance or not.
    """

    def __init__(self, name, message_function, query_rate, prod=False, dry_run=False):
        self.name = name
        self.message_function = message_function
        self.query_rate = query_rate
        self.prod = prod
        self._dry_run = dry_run
        # Time to wait before restarting monitor after an unhandled exception. Exponential backoff.
        self.monitor_reset_delay = RESET_MONITOR_DELAY_INIT
        self._thread_active = False
        self._thread_wrapper = threading.Thread(
            target=self._thread_wrapper_method)

    @abstractmethod
    def _monitor_method(self):
        pass

    def start(self):
        logging.info(f'Starting {self.name} monitoring thread...')
        if self._dry_run:
            self.message_function(
                f'{self.name} monitoring started (with simulated data).')
        elif not self.prod:
            self.message_function(f'{self.name} monitoring started.')
        self._thread_active = True
        self._thread_wrapper.start()

    def stop(self):
        logging.info(f'Stopping {self.name} monitoring thread...')
        if not self.prod:
            self.message_function(f'{self.name} monitoring stopped.')
        self._thread_active = False
        self._thread_wrapper.join(3 * self.query_rate)

    def _thread_wrapper_method(self):
        """
        If an unhandled exception occurs in the monitor and it is killed, log the exception here
        and restart the monitor.

        The most common failures are web3 calls, which can fail arbitrarily on external access.
        """
        retry_time = 0
        while self._thread_active:
            if time.time() < retry_time:
                time.sleep(0.5)
                continue
            try:
                self._monitor_method()
            except Exception as e:
                logging.exception(e)
                logging.error(f'Unhandled exception in the {self.name} thread.'
                              f'\nLogging and **restarting the monitor**.')
            # Reset the reset delay after a stretch of successful running.
            if time.time() > retry_time + 3600:
                self.monitor_reset_delay = RESET_MONITOR_DELAY_INIT
            else:
                self.monitor_reset_delay *= 2
            retry_time = time.time() + self.monitor_reset_delay


class PegCrossMonitor(Monitor):
    """Monitor bean graph for peg crosses and send out messages on detection."""

    def __init__(self, message_function, prod=False):
        super().__init__('Peg', message_function, PEG_CHECK_PERIOD, prod=prod, dry_run=False)
        self.bean_graph_client = BeanSqlClient()
        self.last_known_cross = None

    def _monitor_method(self):
        """Continuously monitor for BEAN price crossing the peg.

        Note that this assumes that block time > period of graph checks.
        """
        # Delay startup to protect against crash loops.
        min_update_time = time.time() + 1
        while self._thread_active:
            # Attempt to check as quickly as the graph allows, but no faster than set frequency.
            if not time.time() > min_update_time:
                time.sleep(1)
                continue
            min_update_time = time.time() + PEG_CHECK_PERIOD

            cross_types = self._check_for_peg_crosses()
            for cross_type in cross_types:
                if cross_type != PegCrossType.NO_CROSS:
                    output_str = PegCrossMonitor.peg_cross_string(cross_type)
                    self.message_function(output_str)

    def _check_for_peg_crosses(self):
        """
        Check to see if the peg has been crossed since the last known timestamp of the caller.
        Assumes that block time > period of graph checks.

        Returns:
            [PegCrossType]
        """
        # Get latest data from graph.
        last_cross = self.bean_graph_client.last_cross()

        # # For testing.
        # import random
        # self.last_known_cross = {'timestamp': 1, 'id': int(last_cross['id']) - 2}
        # logging.info(f'TESTING: Last cross was above? {last_cross["above"]}')
        # price = random.uniform(0.5, 1.5)

        # If the last known cross has not been set yet, initialize it.
        if not self.last_known_cross:
            logging.info('Peg cross timestamp initialized with last peg cross = '
                         f'{last_cross[TIMESTAMP_KEY]}')
            self.last_known_cross = last_cross
            return [PegCrossType.NO_CROSS]

        # If the cross is not newer than the last known cross, return.
        if last_cross[TIMESTAMP_KEY] <= self.last_known_cross[TIMESTAMP_KEY]:
            return [PegCrossType.NO_CROSS]

        # If multiple crosses have occurred since last known cross.
        last_cross_id = int(last_cross['id'])
        last_known_cross_id = int(self.last_known_cross['id'])
        number_of_new_crosses = last_cross_id - last_known_cross_id
        if number_of_new_crosses > 1:
            # Returns n crosses ordered most recent -> least recent.
            new_cross_list = self.bean_graph_client.get_last_crosses(
                n=number_of_new_crosses)
        else:
            new_cross_list = [last_cross]

        # We cannot rely on very recent data of the subgraph to be accurate/consistent. So double
        # check the id and try again later if it is wrong.
        if int(new_cross_list[0]['id']) != last_known_cross_id + number_of_new_crosses:
            logging.warning(f'Subgraph data discrepency on latest peg crosses. Latest cross id '
                            f'is {new_cross_list[0]["id"]} but expected id of {last_cross_id}. '
                            'Trying again later.')
            return [PegCrossType.NO_CROSS]

        # Set the last known cross to be the latest new cross.
        self.last_known_cross = last_cross

        # At least one new cross has been detected.
        # Determine the cross types and return list in ascending order.
        cross_types = []
        for cross in reversed(new_cross_list):
            if cross['above']:
                logging.info('Price crossed above peg.')
                cross_types.append(PegCrossType.CROSS_ABOVE)
            else:
                logging.info('Price crossed below peg.')
                cross_types.append(PegCrossType.CROSS_BELOW)
        return cross_types

    @abstractmethod
    def peg_cross_string(cross_type):
        """Return peg cross string used for bot messages."""
        # NOTE(funderberker): Have to compare enum values here because method of import of caller
        # can change the enum id.
        if cross_type.value == PegCrossType.CROSS_ABOVE.value:
            return '🟩↗ BEAN crossed above peg!'
        elif cross_type.value == PegCrossType.CROSS_BELOW.value:
            return '🟥↘ BEAN crossed below peg!'
        else:
            return 'Peg not crossed.'


class PreviewMonitor(Monitor):
    """Monitor data that offers a peek into current Bean status and update bot name/status."""

    def __init__(self, name_function, status_function):
        super().__init__('Price', status_function,
                         PRICE_CHECK_PERIOD, prod=True, dry_run=False)
        self.STATUS_DISPLAYS_COUNT = 3
        self.HOURS = 24
        self.bean_client = eth_chain.BeanClient()
        self.beanstalk_graph_client = BeanstalkSqlClient()
        self.last_status = ''
        self.status_display_index = 0
        self.name_function = name_function
        self.status_function = status_function

    def _monitor_method(self):
        # Delay startup to protect against crash loops.
        min_update_time = time.time() + 1
        while self._thread_active:
            # Attempt to check as quickly as the graph allows, but no faster than set period.
            if not time.time() > min_update_time:
                time.sleep(1)
                continue
            min_update_time = time.time() + PRICE_CHECK_PERIOD

            bean_price = self.bean_client.avg_bean_price()
            status_str = f'BEAN: ${round_num(bean_price, 4)}'
            if status_str != self.last_status:
                self.name_function(status_str)
                self.last_status = status_str

            # Rotate data and update status.
            self.status_display_index = (
                self.status_display_index + 1) % self.STATUS_DISPLAYS_COUNT
            if self.status_display_index == 0:
                seasons = self.beanstalk_graph_client.seasons_stats(
                    list(range(self.HOURS)), fields=['price'])
                prices = [float(season['price']) for season in seasons]
                self.status_function(
                    f'${round_num(sum(prices) / self.HOURS, 4)} Avg Price - {self.HOURS}hr')
            elif self.status_display_index in [1, 2]:
                seasons = self.beanstalk_graph_client.seasons_stats(
                    list(range(self.HOURS)), fields=['newFarmableBeans', 'newHarvestablePods'])
                mints = [float(season['newFarmableBeans']) +
                         float(season['newHarvestablePods']) for season in seasons]
                if self.status_display_index == 1:
                    self.status_function(
                        f'{round_num(sum(mints)/self.HOURS, 0)} Avg Minted - {self.HOURS}hr')
                if self.status_display_index == 2:
                    self.status_function(
                        f'{round_num(sum(mints), 0)} Minted - {self.HOURS}hr')


class SunriseMonitor(Monitor):
    def __init__(self, message_function, short_msgs=False, channel_to_wallets=None, prod=False):
        super().__init__('Sunrise', message_function,
                         SUNRISE_CHECK_PERIOD, prod=prod, dry_run=False)
        # Toggle shorter messages (must fit into <280 character safely).
        self.short_msgs = short_msgs
        # Read-only access to self.channel_to_wallets, which may be modified by other threads.
        self.channel_to_wallets = channel_to_wallets
        self.beanstalk_graph_client = BeanstalkSqlClient()
        self.bean_client = eth_chain.BeanClient()
        # Most recent season processed. Do not initialize.
        self.current_season_id = None

    def _monitor_method(self):
        while self._thread_active:
            # Wait until the eligible for a sunrise.
            self._wait_until_expected_sunrise()
            # Once the sunrise is complete, get the season stats.
            current_season_stats, last_season_stats = self._block_and_get_seasons_stats()
            # Report season summary to users.
            if current_season_stats:
                self.message_function(self.season_summary_string(
                    last_season_stats, current_season_stats, short_str=self.short_msgs))
            if self.channel_to_wallets:
                self.update_all_wallet_watchers()

            # # For testing.
            # current_season_stats, last_season_stats = self.beanstalk_graph_client.seasons_stats()
            # self.message_function(self.season_summary_string(last_season_stats, current_season_stats, short_str=self.short_msgs))
            # time.sleep(5)

    def _wait_until_expected_sunrise(self):
        """Wait until beanstalk is eligible for a sunrise call.

        Assumes sunrise timing cycle beings with Unix Epoch (1/1/1970 00:00:00 UTC).
        This is not exact since we do not bother with syncing local and graph time.
        """
        seconds_until_next_sunrise = SEASON_DURATION - time.time() % SEASON_DURATION
        sunrise_ready_timestamp = time.time() + seconds_until_next_sunrise
        loop_count = 0
        while self._thread_active and time.time() < sunrise_ready_timestamp:
            if loop_count % 60 == 0:
                logging.info(f'Blindly waiting {int((sunrise_ready_timestamp - time.time())/60)} '
                             'more minutes until expected sunrise.')
            loop_count += 1
            time.sleep(1)

    def _block_and_get_seasons_stats(self):
        """Blocks until sunrise is complete, then returns stats of current and previous season.

        Repeatedly makes graph calls to check sunrise status.
        """
        # TODO(funderberker): Put in max number of checks here before giving up and wait for
        # next sunrise.
        while self._thread_active:
            current_season_stats, last_season_stats = self.beanstalk_graph_client.seasons_stats()
            # If a new season is detected and sunrise was sufficiently recent.
            if (self.current_season_id != current_season_stats['id'] and
                    int(current_season_stats['timestamp']) > time.time() - SEASON_DURATION / 2):
                self.current_season_id = current_season_stats['id']
                logging.info(
                    f'New season detected with id {self.current_season_id}')
                return current_season_stats, last_season_stats
            time.sleep(SUNRISE_CHECK_PERIOD)
        return None, None

    def season_summary_string(self, last_season_stats, current_season_stats, short_str=False):
        new_farmable_beans = float(current_season_stats['newFarmableBeans'])
        new_harvestable_pods = float(
            current_season_stats['newHarvestablePods'])
        newMintedBeans = new_farmable_beans + new_harvestable_pods
        # newSoil = float(current_season_stats['newSoil'])
        new_deposited_lp = float(last_season_stats["newDepositedLP"])
        new_withdrawn_lp = float(last_season_stats["newWithdrawnLP"])
        pooled_eth = float(current_season_stats['pooledEth'])
        pooled_beans = float(current_season_stats['pooledBeans'])
        total_lp = float(current_season_stats['lp'])
        # bean_pool_ratio = pooled_beans / total_lp
        # eth_pool_ratio = pooled_eth / total_lp
        # deposited_bean_lp = round_num(new_deposited_lp * bean_pool_ratio)
        # deposited_eth_lp = round_num(new_deposited_lp * eth_pool_ratio)
        deposited_eth_lp, deposited_bean_lp = lp_eq_values(
            new_deposited_lp, total_lp=total_lp, pooled_eth=pooled_eth, pooled_beans=pooled_beans)
        # withdrawn_bean_lp = round_num(new_withdrawn_lp * bean_pool_ratio)
        # withdrawn_eth_lp = round_num(new_withdrawn_lp * eth_pool_ratio)
        withdrawn_eth_lp, withdrawn_bean_lp = lp_eq_values(
            new_withdrawn_lp, total_lp=total_lp, pooled_eth=pooled_eth, pooled_beans=pooled_beans)
        last_weather = float(last_season_stats['weather'])
        newPods = float(last_season_stats['newPods'])

        ret_string = f'⏱ Season {last_season_stats["id"]} is complete!'
        ret_string += f'\n💵 The TWAP last season was ${round_num(current_season_stats["price"], 3)}'
        ret_string += f'\n🌤 The weather is {current_season_stats["weather"]}%'
        # ret_string += f'\nThere is {current_season_stats["soil"]} soil available' # Coming in graph version 1.1.10
        if newMintedBeans:
            if not short_str:
                ret_string += f'\n\n🌱 {round_num(newMintedBeans)} Beans were minted'
                ret_string += f'\n👩‍🌾 {round_num(new_farmable_beans)} Beans are newly farmable'
                ret_string += f'\n👨‍🌾 {round_num(new_harvestable_pods)} Pods are newly harvestable'
            else:
                ret_string += f'\n\n🌱 {round_num(newMintedBeans)} Beans were minted between the silo and field'
        else:
            ret_string += f'\n\n🌱 No new Beans were minted.'
        # if newSoil:
        #     ret_string += f'\n\n{round_num(newSoil)} soil was added'
        if not short_str:
            ret_string += f'\n\n📥 {round_num(last_season_stats["newDepositedBeans"])} Beans deposited'
            ret_string += f'\n📥 {round_num(deposited_bean_lp)} Beans and {round_num(deposited_eth_lp)} ETH of LP deposited'
            ret_string += f'\n📤 {round_num(last_season_stats["newWithdrawnBeans"])} Beans withdrawn'
            ret_string += f'\n📤 {round_num(withdrawn_bean_lp)} Beans and {round_num(withdrawn_eth_lp)} ETH of LP withdrawn'
            ret_string += f'\n🚜 {round_num(newPods / (1 + last_weather/100))} Beans sown'
            ret_string += f'\n🌾 {round_num(newPods)} Pods minted'
            ret_string += '\n_ _'  # empty line that does not get stripped
        else:
            ret_string += f'\n\n📥 {round_num(float(last_season_stats["newDepositedBeans"]) + deposited_bean_lp * 2)} Beans worth of deposits'
            ret_string += f'\n📤 {round_num(float(last_season_stats["newWithdrawnBeans"]) + withdrawn_bean_lp * 2)} Beans worth of withdraws'
            ret_string += f'\n🚜 {round_num(newPods / (1 + last_weather/100))} Beans sown for {round_num(newPods)} pods'

        return ret_string

    def update_all_wallet_watchers(self):
        current_season_stats = self.beanstalk_graph_client.current_season_stats()
        bean_price = self.bean_client.avg_bean_price()
        for channel_id, wallets in self.channel_to_wallets.items():
            # Ignore users with empty watch lists.
            if not wallets:
                continue
            self.message_function(self.wallets_str(
                wallets, current_season_stats, bean_price), channel_id)

    def wallets_str(self, wallets, current_season_stats, bean_price):
        ret_str = ''
        account_id_to_addr = {str.lower(addr): addr for addr in wallets}
        accounts_status = self.beanstalk_graph_client.wallets_stats(
            list(account_id_to_addr.keys()))
        for account_status in accounts_status:
            ret_str += self.wallet_str(
                account_id_to_addr[account_status['id']], account_status, current_season_stats, bean_price)
            ret_str += '\n'
        return ret_str

    def wallet_str(self, address, account_status, current_season_stats, bean_price):
        """Create a standard string representing a wallet status.

        address is a string of the wallet address (with standard capitalization).
        account_stats is a map of data about an account from the subgraph.
        """
        deposited_beans = float(account_status["depositedBeans"])
        lp_eth, lp_beans = lp_eq_values(
            float(account_status["depositedLP"]),
            total_lp=float(current_season_stats['lp']),
            pooled_eth=float(current_season_stats['pooledEth']),
            pooled_beans=float(current_season_stats['pooledBeans']),
        )

        ret_string = f'📜 `{address}`\n'
        # wallet_str += f'Circulating Beans: {account_stats[""]}'
        ret_string += f'🌱 Deposited Beans: {round_num(deposited_beans)}  (${round_num(deposited_beans*bean_price)})\n'
        ret_string += f'🌿 Deposited LP: {round_num(lp_eth, 4)} ETH and {round_num(lp_beans)} Beans  (${round_num(2*lp_beans*bean_price)})\n'
        ret_string += f'🌾 Pods: {round_num(account_status["pods"])}\n'
        return ret_string


class UniswapPoolMonitor(Monitor):
    """Monitor the ETH:BEAN Uniswap V2 pool for events."""

    def __init__(self, message_function, prod=False, dry_run=False):
        super().__init__('Uniswap Pool', message_function,
                         POOL_CHECK_RATE, prod=prod, dry_run=dry_run)
        self._eth_event_client = eth_chain.EthEventsClient(
            eth_chain.EventClientType.UNISWAP_POOL)
        self.uniswap_client = eth_chain.UniswapClient()
        self.bean_client = eth_chain.BeanClient()

    def _monitor_method(self):
        last_check_time = 0
        while self._thread_active:
            if time.time() < last_check_time + POOL_CHECK_RATE:
                time.sleep(0.5)
                continue
            last_check_time = time.time()
            for txn_hash, event_logs in self._eth_event_client.get_new_logs(dry_run=self._dry_run).items():
                self._handle_txn_logs(txn_hash, event_logs)

            # # For testing purposes, track the price on each check.
            # if not self.prod:
            #     self.uniswap_client.current_eth_and_bean_price()

    def _handle_txn_logs(self, txn_hash, event_logs):
        """Process the pool event logs for a single txn.

        Assumes that there are not non-Bean swaps in logs (e.g. ETH:USDC).
        Note that Event Log Object is not the same as Event object. *sideeyes web3.py developers.*
        """
        # Match the txn invoked method. Matching is done on the first 10 characters of the hash.
        transaction = self.uniswap_client._web3.eth.get_transaction(txn_hash)
        txn_method_sig_prefix = transaction['input'][:9]

        # Process the txn logs based on the method.
        # Ignore silo conversion events. They will be handled by the beanstalk class.
        if sig_compare(txn_method_sig_prefix, eth_chain.silo_conversion_sigs.values()):
            return
        # No special logic for deposits. If they include a swap we should process it as normal.
        elif sig_compare(txn_method_sig_prefix, eth_chain.bean_deposit_sigs.values()):
            pass
        else:
            # All other txn log sets should include a standard ETH:BEAN swap.
            pass

        # Each txn of interest should only include one ETH:BEAN swap.
        if len(event_logs) > 1:
            logging.warning(
                f'Multiple swaps of interest seen in a single txn ({str(event_logs)}).')
        for event_log in event_logs:
            event_str = UniswapPoolMonitor.any_event_str(
                event_log, self.uniswap_client.current_eth_price(),
                self.bean_client.uniswap_v2_bean_price())
            if event_str:
                self.message_function(event_str)

    @abstractmethod
    def any_event_str(event_log, eth_price, bean_price):
        event_str = ''
        # Parse possible values of interest from the event log. Not all will be populated.
        eth_amount = eth_chain.eth_to_float(event_log.args.get('amount0'))
        bean_amount = eth_chain.bean_to_float(event_log.args.get('amount1'))
        eth_in = eth_chain.eth_to_float(event_log.args.get('amount0In'))
        eth_out = eth_chain.eth_to_float(event_log.args.get('amount0Out'))
        bean_in = eth_chain.bean_to_float(event_log.args.get('amount1In'))
        bean_out = eth_chain.bean_to_float(event_log.args.get('amount1Out'))

        if event_log.event in ['Mint', 'Burn']:
            if event_log.event == 'Mint':
                event_str += f'📥 LP added - {round_num(bean_amount)} Beans and {round_num(eth_amount, 4)} ETH'
            if event_log.event == 'Burn':
                event_str += f'📤 LP removed - {round_num(bean_amount)} Beans and {round_num(eth_amount, 4)} ETH'
            # LP add/remove always takes equal value of both assets.
            lp_value = bean_amount * bean_price * 2
            event_str += f' (${round_num(lp_value)})'
            event_str += f'\n{value_to_emojis(lp_value)}'
        elif event_log.event == 'Swap':
            if eth_in:
                event_str += UniswapPoolMonitor.swap_event_str(
                    eth_price, bean_price, eth_in=eth_in, bean_out=bean_out)
            elif bean_in:
                event_str += UniswapPoolMonitor.swap_event_str(
                    eth_price, bean_price, bean_in=bean_in, eth_out=eth_out)

        event_str += f'\n<https://etherscan.io/tx/{event_log.transactionHash.hex()}>'
        # empty line that does not get stripped
        event_str += '\n_ _'
        return event_str

    @abstractmethod
    def swap_event_str(eth_price, bean_price, eth_in=None, bean_in=None, eth_out=None, bean_out=None):
        event_str = ''
        if ((not eth_in and not bean_in) or (not eth_out and not bean_out)):
            logging.error(
                'Must set at least one input and one output of swap.')
            return ''
        if ((eth_in and bean_in) or (eth_out and bean_out)):
            logging.error('Cannot set two inputs or two outputs of swap.')
            return ''
        if eth_in:
            event_str += f'📗 {round_num(bean_out)} Beans bought for {round_num(eth_in, 4)} ETH'
            swap_price = eth_chain.avg_eth_to_bean_swap_price(
                eth_in, bean_out, eth_price)
            swap_value = swap_price * bean_out
        elif bean_in:
            event_str += f'📕 {round_num(bean_in)} Beans sold for {round_num(eth_out, 4)} ETH'
            swap_price = eth_chain.avg_bean_to_eth_swap_price(
                bean_in, eth_out, eth_price)
            swap_value = swap_price * bean_in
        event_str += f' @ ${round_num(swap_price, 4)} (${round_num(swap_value)})'
        event_str += f'  -  Latest block price is ${round_num(bean_price, 4)}'
        event_str += f'\n{value_to_emojis(swap_value)}'
        return event_str


class CurvePoolMonitor(Monitor):
    """Monitor the BEAN:3CRV Curve pool for events."""

    def __init__(self, message_function, prod=False, dry_run=False):
        super().__init__('Curve Pool', message_function,
                         POOL_CHECK_RATE, prod=prod, dry_run=dry_run)
        self._eth_event_client = eth_chain.EthEventsClient(
            eth_chain.EventClientType.CURVE_POOL)
        self.bean_client = eth_chain.BeanClient()

    def _monitor_method(self):
        last_check_time = 0
        while self._thread_active:
            if time.time() < last_check_time + POOL_CHECK_RATE:
                time.sleep(0.5)
                continue
            last_check_time = time.time()
            for txn_hash, event_logs in self._eth_event_client.get_new_logs(dry_run=self._dry_run).items():
                self._handle_txn_logs(txn_hash, event_logs)

    def _handle_txn_logs(self, txn_hash, event_logs):
        """Process the curve pool event logs for a single txn.

        Assumes that there are no non-Bean:3CRV TokenExchangeUnderlying events in logs.
        Note that Event Log Object is not the same as Event object.
        """
        for event_log in event_logs:
            event_str = CurvePoolMonitor.any_event_str(
                event_log, self.bean_client.curve_bean_price())
            if event_str:
                self.message_function(event_str)

    @abstractmethod
    def any_event_str(event_log, bean_price):
        event_str = ''
        # Parse possible values of interest from the event log. Not all will be populated.
        sold_id = event_log.args.get('sold_id')
        tokens_sold = event_log.args.get('tokens_sold')
        bought_id = event_log.args.get('bought_id')
        tokens_bought = event_log.args.get('tokens_bought')
        token_amounts = event_log.args.get('token_amounts')
        # Coin is the non-pool token non-crv token, Bean. Unclear why this is the naming Curve used.
        coin_amount = event_log.args.get('coin_amount')

        if token_amounts is not None:
            bean_lp_amount = eth_chain.bean_to_float(
                token_amounts[eth_chain.FACTORY_INDEX_BEAN])
            crv_lp_amount = eth_chain.crv_to_float(
                token_amounts[eth_chain.FACTORY_INDEX_3CRV])
        if coin_amount is not None:
            coin_lp_amount = eth_chain.bean_to_float(coin_amount)

        if event_log.event == 'TokenExchangeUnderlying' or event_log.event == 'TokenExchange':
            # Set the variables of quantity and direction of exchange.
            bean_out = stable_in = bean_in = stable_out = None
            if bought_id == eth_chain.FACTORY_UNDERLYING_INDEX_BEAN:
                bean_out = eth_chain.bean_to_float(tokens_bought)
                stable_in = tokens_sold
                stable_id = sold_id
            elif sold_id == eth_chain.FACTORY_UNDERLYING_INDEX_BEAN:
                bean_in = eth_chain.bean_to_float(tokens_sold)
                stable_out = tokens_bought
                stable_id = bought_id
            else:
                logging.error(
                    'Exchange detected between two non-Bean tokens. Ignoring.')
                return ''

            # Set the stable name string and convert value to float.
            if event_log.event == 'TokenExchange':
                stable_name = '3CRV'
                stable_in = eth_chain.crv_to_float(stable_in)
                stable_out = eth_chain.crv_to_float(stable_out)
            elif stable_id == eth_chain.FACTORY_UNDERLYING_INDEX_DAI:
                stable_name = 'DAI'
                stable_in = eth_chain.dai_to_float(stable_in)
                stable_out = eth_chain.dai_to_float(stable_out)
            elif stable_id == eth_chain.FACTORY_UNDERLYING_INDEX_USDC:
                stable_name = 'USDC'
                stable_in = eth_chain.usdc_to_float(stable_in)
                stable_out = eth_chain.usdc_to_float(stable_out)
            elif stable_id == eth_chain.FACTORY_UNDERLYING_INDEX_USDT:
                stable_name = 'USDT'
                stable_in = eth_chain.usdt_to_float(stable_in)
                stable_out = eth_chain.usdt_to_float(stable_out)
            else:
                logging.error(
                    f'Unexpected stable_id seen ({stable_id}) in exchange. Ignoring.')
                return ''

            event_str += CurvePoolMonitor.exchange_event_str(event_log, bean_price, stable_name,
                                                             bean_out=bean_out, bean_in=bean_in,
                                                             stable_in=stable_in, stable_out=stable_out)
        elif event_log.event == 'AddLiquidity':
            event_str += f'📥 LP added - {round_num(bean_lp_amount)} Beans and {round_num(crv_lp_amount)} 3CRV'
        elif event_log.event == 'RemoveLiquidity' or event_log.event == 'RemoveLiquidityImbalance':
            event_str += f'📤 LP removed - {round_num(bean_lp_amount)} Beans and {round_num(crv_lp_amount, 4)} 3CRV'
        elif event_log.event == 'RemoveLiquidityOne':
            event_str += f'📤 LP removed - {round_num(coin_lp_amount)} Beans'
        else:
            logging.warning(
                f'Unexpected event log seen in Curve Pool ({event_log.event}). Ignoring.')
            return ''

        event_str += f'\n<https://etherscan.io/tx/{event_log.transactionHash.hex()}>'
        # empty line that does not get stripped
        event_str += '\n_ _'
        return event_str

    @abstractmethod
    def exchange_event_str(event_log, bean_price, stable_name, stable_in=None, bean_in=None, stable_out=None, bean_out=None):
        """Generate a standard token exchange string.

        Note that we assume all tokens in 3CRV have a value of $1.
        """
        event_str = ''
        if ((not stable_in and not bean_in) or (not stable_out and not bean_out)):
            logging.error(
                'Must set at least one input and one output of swap.')
            return ''
        if ((stable_in and bean_in) or (stable_out and bean_out)):
            logging.error('Cannot set two inputs or two outputs of swap.')
            return ''
        if stable_in:
            event_str += f'📗 {round_num(bean_out)} Beans bought for {round_num(stable_in)} {stable_name}'
            swap_price = stable_in / bean_out
            swap_value = stable_in
        elif bean_in:
            event_str += f'📕 {round_num(bean_in)} Beans sold for {round_num(stable_out)} {stable_name}'
            swap_price = stable_out / bean_in
            swap_value = stable_out
        event_str += f' @ ${round_num(swap_price, 4)} (${round_num(swap_value)})'
        event_str += f'  -  Latest block price is ${round_num(bean_price, 4)}'
        event_str += f'\n{value_to_emojis(swap_value)}'
        return event_str


class BeanstalkMonitor(Monitor):
    """Monitor the Beanstalk contract for events."""

    def __init__(self, message_function, prod=False, dry_run=False):
        super().__init__('Beanstalk', message_function,
                         BEANSTALK_CHECK_RATE, prod=prod, dry_run=dry_run)
        self._web3 = eth_chain.get_web3_instance()
        self._eth_event_client = eth_chain.EthEventsClient(
            eth_chain.EventClientType.BEANSTALK)
        self.beanstalk_graph_client = BeanstalkSqlClient()
        self.bean_client = eth_chain.BeanClient()
        self.uniswap_client = eth_chain.UniswapClient()

    def _monitor_method(self):
        last_check_time = 0
        while self._thread_active:
            if time.time() < last_check_time + BEANSTALK_CHECK_RATE:
                time.sleep(0.5)
                continue
            last_check_time = time.time()
            for txn_hash, event_logs in self._eth_event_client.get_new_logs(dry_run=self._dry_run).items():
                self._handle_txn_logs(txn_hash, event_logs)

    def _handle_txn_logs(self, txn_hash, event_logs):
        """Process the beanstalk event logs for a single txn.

        Note that Event Log Object is not the same as Event object.
        """
        # Match the txn invoked method. Matching is done on the first 10 characters of the hash.
        transaction = self._web3.eth.get_transaction(
            txn_hash)
        txn_method_sig_prefix = transaction['input'][:9]

        # Prune embedded bean deposit logs. They are uninteresting clutter.
        last_bean_deposit = None
        bean_deposit_logs = []
        for event_log in event_logs:
            # Parse through BeanDeposit logs, keeping track of which is last.
            if event_log.event == 'BeanDeposit':
                if last_bean_deposit is None or event_log.logIndex > last_bean_deposit.logIndex:
                    last_bean_deposit = event_log
                bean_deposit_logs.append(event_log)
        # Remove all bean_deposit logs from the log list.
        for event_log in bean_deposit_logs:
            # Remove this log from the list.
            event_logs.remove(event_log)

        # NOTE(funderberker): Can this logic that uses the txn method be pushed down lower with
        # use of the txn receipt?
        # Process some txn logs as groups, based on the contract function signature.
        # Compile all events within a silo conversion to a single action.
        if sig_compare(txn_method_sig_prefix, eth_chain.silo_conversion_sigs.values()):
            logging.info(f'Silo conversion txn seen ({txn_hash.hex()}).')
            # If this is a conversion into beans, include last bean deposit log.
            if sig_compare(txn_method_sig_prefix, eth_chain.silo_conversion_sigs['convertDepositedLP']):
                event_logs.append(last_bean_deposit)
            self.message_function(self.silo_conversion_str(
                event_logs, self.beanstalk_graph_client))
            return
        # If there is a direct bean deposit, do not ignore the last bean deposit event.
        elif sig_compare(txn_method_sig_prefix, eth_chain.bean_deposit_sigs.values()):
            logging.info(f'Bean deposit txn seen ({txn_hash.hex()}).')
            # Include last bean deposit log for this type of txn.
            event_logs.append(last_bean_deposit)

        # Handle txn logs individually using default strings.
        for event_log in event_logs:
            event_str = self.any_event_str(event_log,
                                           self.beanstalk_graph_client)
            self.message_function(event_str)

    def any_event_str(self, event_log, beanstalk_graph_client):
        event_str = ''

        # Pull args from the event log. Not all will be populated.
        token_address = event_log.args.get('token')
        eth_price = self.uniswap_client.current_eth_price()
        bean_price = self.bean_client.avg_bean_price()
        lp_amount = eth_chain.lp_to_float(event_log.args.get('lp'))
        if lp_amount:
            lp_eth, lp_beans = lp_eq_values(
                lp_amount, beanstalk_graph_client=beanstalk_graph_client)
            token_address = UNI_V2_BEAN_ETH_ADDR
            lp_value = lp_eth * eth_price + lp_beans * bean_price
        beans_amount = eth_chain.bean_to_float(event_log.args.get('beans'))
        if beans_amount:
            beans_value = beans_amount * bean_price
            token_address = BEAN_ADDR
        pods_amount = eth_chain.bean_to_float(event_log.args.get('pods'))
        token_amount_long = event_log.args.get('amount')
        token_amounts_long = event_log.args.get('amounts')
        bdv = eth_chain.bean_to_float(event_log.args.get('bdv'))
        bdv_value = bdv * bean_price

        # Ignore these events. They are uninteresting clutter.
        if event_log.event in ['BeanRemove', 'LPRemove', 'RemoveSeason', 'RemoveSeasons']:
            return ''
        
        # # LP Events.
        # # NOTE(funderberker): These events will be deprecated eventually in favor of generalized
        # # Deposit and Withdraw.
        # elif event_log.event in ['LPDeposit', 'LPWithdraw']:
        #     if event_log.event == 'LPDeposit':
        #         event_str += f'📥 Uniswap LP Deposit'
        #     elif event_log.event == 'LPWithdraw':
        #         event_str += f'📭 Uniswap LP Withdrawal'
        #     event_str += f' - {round_num(lp_beans)} Beans and {round_num(lp_eth,4)} ETH (${round_num(lp_value)})'
        #     event_str += f'\n{value_to_emojis(lp_value)}'
        
        # Deposit & Withdraw events.
        # NOTE(funderberker): Bean and LP specific events will be deprecated eventually in favor of
        # generalized Deposit and Withdraw.
        elif event_log.event in ['Deposit', 'Withdraw', 'BeanDeposit', 'BeanWithdraw', 'LPDeposit', 'LPWithdraw']:
            token_name, token_symbol, decimals = eth_chain.get_erc20_info(token_address, web3=self._web3)
            # If this event is a BeanDeposit or BeanWithdraw.
            if beans_amount:
                amount = beans_amount
                value = beans_value
            elif lp_amount:
                amount = lp_amount
                value = lp_value
            else:
                amount = eth_chain.token_to_float(token_amount_long or sum(token_amounts_long), decimals)
                value = bdv_value
            
            if event_log.event in ['Deposit', 'BeanDeposit', 'LPDeposit']:
                event_str += f'📥 Silo Deposit'
            elif event_log.event in ['Withdraw', 'BeanWithdraw', 'LPWithdraw']:
                event_str += f'📭 Silo Withdrawal'
            event_str += f' - {round_num_auto(amount)} {token_symbol}'
            # NOTE(funderberker): Value is not known for generalized withdrawals. This should be
            # fixed when more infrastructure is in place.
            if value:
                event_str += f' (${round_num(value)})'
                event_str += f'\n{value_to_emojis(value)}'
        # Sow event.
        elif event_log.event == 'Sow':
            event_str += f'🚜 {round_num(beans_amount)} Beans sown for ' \
                f'{round_num(pods_amount)} Pods (${round_num(beans_value)})'
            event_str += f'\n{value_to_emojis(beans_value)}'
        elif event_log.event == 'Harvest':
            event_str += f'👩‍🌾 {round_num(beans_amount)} Pods harvested for Beans (${round_num(beans_value)})'
            event_str += f'\n{value_to_emojis(beans_value)}'
        else:
            logging.warning(
                f'Unexpected event log from Beanstalk contract ({event_log}). Ignoring.')
            return ''

        event_str += f'\n<https://etherscan.io/tx/{event_log.transactionHash.hex()}>'
        # empty line that does not get stripped
        event_str += '\n_ _'
        return event_str

    def silo_conversion_str(self, event_logs, beanstalk_graph_client):
        """Create a human-readable string representing a silo position conversion.

        Assumes that there are no non-Bean swaps contained in the event logs.
        Assumes event_logs is not empty.
        Assumes embedded beanDeposits have been removed from logs.
        Uses events from Beanstalk contract.
        """
        beans_converted = lp_converted = None
        bean_price = self.bean_client.avg_bean_price()
        # Find the relevant logs (Swap + Mint/Burn).
        for event_log in event_logs:
            # One Swap event will always be present.
            # But we cannot parse the Swap event because it is only seen by the pool monitor.
            # if event_log.event == 'Swap':

            # One of the below two events will always be present.
            if event_log.event == 'BeanRemove':
                beans_converted = eth_chain.bean_to_float(
                    event_log.args.get('beans'))
            elif event_log.event == 'LPRemove':
                lp_converted = eth_chain.lp_to_float(event_log.args.get('lp'))
                lp_converted_eth, lp_converted_beans = lp_eq_values(
                    lp_converted, beanstalk_graph_client=beanstalk_graph_client)

            # One of the below two events will always be present.
            elif event_log.event == 'BeanDeposit':
                beans_deposited = eth_chain.bean_to_float(
                    event_log.args.get('beans'))
                value = beans_deposited * bean_price
            elif event_log.event == 'LPDeposit':
                lp_deposited = eth_chain.lp_to_float(event_log.args.get('lp'))
                lp_deposited_eth, lp_deposited_beans = lp_eq_values(
                    lp_deposited, beanstalk_graph_client=beanstalk_graph_client)
                value = lp_deposited_beans * 2 * bean_price

            # elif event_log.event == 'Burn':
            # elif event_log.event == 'Mint':

        # If converting to LP.
        if beans_converted:
            event_str = f'🔃 {round_num(beans_converted)} siloed Beans converted to {round_num(lp_deposited_eth,4)} ETH & {round_num(lp_deposited_beans)} Beans of LP (${round_num(value)})'
        # If converting to Beans.
        elif lp_converted:
            event_str = f'🔄 {round_num(lp_converted_eth,4)} ETH and {round_num(lp_converted_beans)} Beans of siloed LP converted to {round_num(beans_deposited)} siloed Beans (${round_num(value)})'

        event_str += f'\nLatest block price is ${round_num(bean_price, 4)}'
        event_str += f'\n{value_to_emojis(value)}'
        event_str += f'\n<https://etherscan.io/tx/{event_logs[0].transactionHash.hex()}>'
        # empty line that does not get stripped
        event_str += '\n_ _'
        return event_str


class MarketMonitor(Monitor):
    """Monitor the Beanstalk contract for market events."""

    def __init__(self, message_function, prod=False, dry_run=False):
        super().__init__('Market', message_function,
                         BEANSTALK_CHECK_RATE, prod=prod, dry_run=dry_run)
        self._eth_event_client = eth_chain.EthEventsClient(
            eth_chain.EventClientType.MARKET)
        self._web3 = eth_chain.get_web3_instance()
        self.bean_client = eth_chain.BeanClient(self._web3)
        self.bean_contract = eth_chain.get_bean_contract(self._web3)
        self.beanstalk_contract = eth_chain.get_beanstalk_contract(self._web3)

    def _monitor_method(self):
        last_check_time = 0
        while self._thread_active:
            if time.time() < last_check_time + BEANSTALK_CHECK_RATE:
                time.sleep(0.5)
                continue
            last_check_time = time.time()
            for txn_hash, event_logs in self._eth_event_client.get_new_logs(dry_run=self._dry_run).items():
                self._handle_txn_logs(txn_hash, event_logs)

    def _handle_txn_logs(self, txn_hash, event_logs):
        """Process the beanstalk event logs for a single txn.

        Note that Event Log Object is not the same as Event object.
        """
        # Match the txn invoked method. Matching is done on the first 10 characters of the hash.
        transaction_receipt = self._web3.eth.get_transaction_receipt(txn_hash)

        # Handle txn logs individually using default strings.
        for event_log in event_logs:
            event_str = self.farmers_market_str(event_log, transaction_receipt)
            event_str += f'\n<https://etherscan.io/tx/{event_logs[0].transactionHash.hex()}>'
            # empty line that does not get stripped
            event_str += '\n_ _'
            self.message_function(event_str)

    def farmers_market_str(self, event_log, transaction_receipt):
        """Create a human-readable string representing an event related to the farmer's market.

        Assumes event_log is an event of one of the types implemented below.
        Uses events from Beanstalk contract.
        """
        event_str = ''
        # Pull args from event logs. Not all will be populated.
        # Amount of pods being listed/ordered.
        amount = eth_chain.pods_to_float(event_log.args.get('amount'))
        price_per_pod = eth_chain.bean_to_float(
            event_log.args.get('pricePerPod'))

        # Index of the plot (place in line of first pod of the plot).
        plot_index = eth_chain.pods_to_float(event_log.args.get('index'))
        # Index of earliest pod to list, relative to start of plot.
        relative_start_index = eth_chain.pods_to_float(event_log.args.get('start'))
        # Absolute index of the first pod to list.
        start_index = plot_index + relative_start_index
        # Current index at start of pod line (number of pods ever harvested).
        pods_harvested = eth_chain.pods_to_float(self.beanstalk_contract.functions.harvestableIndex().call())
        # Lowest place in line of a listing.
        start_place_in_line = start_index - pods_harvested
        # Highest place in line an order will purchase.
        order_max_place_in_line = eth_chain.pods_to_float(event_log.args.get('maxPlaceInLine'))

        bean_price = self.bean_client.avg_bean_price()
        amount_str = round_num(amount, 0)
        price_per_pod_str = round_num(price_per_pod)
        start_place_in_line_str = round_num(start_place_in_line, 0)
        order_max_place_in_line_str = round_num(order_max_place_in_line, 0)

        if event_log.event == 'PodListingCreated':
            # Check if this was a relist.
            listing_cancelled_log = self.beanstalk_contract.events['PodListingCancelled'](
            ).processReceipt(transaction_receipt, errors=eth_chain.DISCARD)
            if listing_cancelled_log:
                event_str += f'♻ Pods relisted'
            else:
                event_str += f'✏ Pods listed'
            event_str += f' - {amount_str} Pods queued at {start_place_in_line_str} listed @ {price_per_pod_str} Beans/Pod (${round_num(amount * bean_price * price_per_pod)})'
        elif event_log.event == 'PodOrderCreated':
            # Check if this was a relist.
            order_cancelled_log = self.beanstalk_contract.events['PodOrderCancelled'](
            ).processReceipt(transaction_receipt, errors=eth_chain.DISCARD)
            if order_cancelled_log:
                event_str += f'♻ Pods reordered'
            else:
                event_str += f'🖌 Pods ordered'
            event_str += f' - {amount_str} Pods queued before {order_max_place_in_line_str} ordered @ {price_per_pod_str} Beans/Pod (${round_num(amount * bean_price * price_per_pod)})'
        elif event_log.event in ['PodListingFilled', 'PodOrderFilled']:
            # Pull the Bean Transfer log to find cost.
            if event_log.event == 'PodListingFilled':
                transfer_logs = self.bean_contract.events['Transfer']().processReceipt(
                    transaction_receipt, errors=eth_chain.DISCARD)
                logging.info(f'Transfer log(s):\n{transfer_logs}')
                # There should be exactly one transfer log of Beans.
                bean_transfer_log = None
                for log in transfer_logs:
                    if log.address == BEAN_ADDR:
                        bean_transfer_log = log
                        break
                if not bean_transfer_log:
                    logging.error(
                        f'Unexpected Transfer event configuration in market fill txn '
                        f'({transaction_receipt.transactionHash}). Exiting...')
                    raise ValueError('Unexpected txn logs')
                beans_paid = eth_chain.bean_to_float(
                    bean_transfer_log.args.get('value'))
            elif event_log.event == 'PodOrderFilled':
                # Get price from original order creation.
                # NOTE(funderberker): This is a lot of duplicate logic from EthEventsClient.get_new_logs()
                # that I simply do not have the bandwidth right now to refactor in a way that can be
                # used in both places.
                beanstalk_contract = eth_chain.get_beanstalk_contract(self._web3)
                event_filter = beanstalk_contract.events.PodOrderCreated.createFilter(
                    fromBlock=14120000, # Feb 1, 2022.
                    toBlock=int(transaction_receipt.blockNumber),
                    argument_filters={'id': event_log.args.get('id')}
                )
                log_entries = event_filter.get_all_entries()
                if len(log_entries) == 0:
                    logging.error('No PodOrderCreated event found. Exiting...')
                    raise ValueError('Failed to locate expected event.')
                if len(log_entries) > 1:
                    logging.error('Too many PodOrderCreated events found. Exiting...')
                    raise ValueError('Unexpected events seen.')
                log_entry = log_entries[0]
                txn_hash = log_entry['transactionHash']
                transaction_receipt = self._web3.eth.get_transaction_receipt(txn_hash)
                decoded_log_entry = beanstalk_contract.events['PodOrderCreated']().processReceipt(
                    transaction_receipt, errors=eth_chain.DISCARD)[0]
                logging.info(decoded_log_entry)
                price_per_pod = decoded_log_entry.args.pricePerPod
                beans_paid = eth_chain.bean_to_float(price_per_pod) * amount
            event_str += f'💰 Pods Exchanged - {amount_str} Pods queued at ' \
                         f'{start_place_in_line_str} purchased @ {round_num(beans_paid/amount)} ' \
                         f'Beans/Pod (${round_num(bean_price * beans_paid)})'
            event_str += f'\n{value_to_emojis(bean_price * beans_paid)}'
        # NOTE(funderberker): There is no way to meaningfully identify what has been cancelled, in
        # terms of amount/cost/etc. We could parse all previous creation events to find matching
        # index, but it is not clear that it is worth it.
        # elif event_log.event == 'PodListingCancelled':
        # elif event_log.event == 'PodOrderCancelled':
        return event_str


class MsgHandler(logging.Handler):
    """A handler class which sends a message on a text channel."""
    def __init__(self, message_function):
        """
        Initialize the handler.
        """
        logging.Handler.__init__(self)
        self.message_function = message_function

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        """
        try:
            msg = self.format(record)
            self.message_function(msg)
        except Exception:
            self.handleError(record)

    def __repr__(self):
        level = getLevelName(self.level)
        return '<%s %s (%s)>' % (self.__class__.__name__, self.baseFilename, level)



def sig_compare(signature, signatures):
    """Compare a signature to one or many signatures and return if there are any matches. 

    Comparison is made based on 10 character prefix.
    """
    if type(signatures) is str:
        return signature[:9] == signatures[:9]

    for sig in signatures:
        if signature[:9] == sig[:9]:
            return True
    return False


def lp_properties(beanstalk_graph_client):
    current_season_stats = beanstalk_graph_client.current_season_stats()
    pooled_eth = float(current_season_stats['pooledEth'])
    pooled_beans = float(current_season_stats['pooledBeans'])
    total_lp = float(current_season_stats['lp'])
    return pooled_eth, pooled_beans, total_lp


def lp_eq_values(lp, total_lp=None, pooled_eth=None, pooled_beans=None, beanstalk_graph_client=None):
    """Return the amount of ETH and beans equivalent to an amount of LP.

    Args:
        total_lp: current amount of lp in pool.
        pooled_eth: current amount of eth in pool.
        pooled_beans: current amount of beans in pool.
        beanstalk_graph_client: a beanstalk graphsql client. If provided latest season stats will
            be retrieved and used.
    """
    if beanstalk_graph_client:
        pooled_eth, pooled_beans, total_lp = lp_properties(
            beanstalk_graph_client)

    if None in [total_lp, pooled_eth, pooled_beans]:
        raise ValueError(
            'Must provide (total_lp & pooled_eth & pooled_beans) OR beanstalk_graph_client')

    bean_pool_ratio = pooled_beans / total_lp
    eth_pool_ratio = pooled_eth / total_lp
    eth_lp = lp * eth_pool_ratio
    bean_lp = lp * bean_pool_ratio
    return eth_lp, bean_lp


def round_num(number, precision=2):
    """Round a string or float to requested precision and return as a string."""
    return f'{float(number):,.{precision}f}'

def round_num_auto(number, sig_fig_min=3, min_precision=2):
    """Round a string or float and return as a string.
    
    Caller specifies the minimum significant figures and precision that that very large and very
    small numbers can both be handled.
    """
    if number > 1:
        return round_num(number, min_precision)
    return '%s' % float(f'%.{sig_fig_min}g' % float(number))

def value_to_emojis(value):
    """Convert a rounded dollar value to a string of emojis."""
    value = int(value)
    if value < 0:
        return ''
    value = round(value, -3)
    if value < 10000:
        return '🐟' * (value // 1000) or '🐟'
    value = round(value, -4)
    if value < 100000:
        return '🦈' * (value // 10000)
    value = round(value, -5)
    return '🐳' * (value // 100000)


def msg_includes_embedded_links(msg):
    """Attempt to detect if there are embedded links in this message. Not an exact system."""
    if msg.count(']('):
        return True


def handle_sigterm(signal_number, stack_frame):
    """Process a sigterm with a python exception for clean exiting."""
    logging.warning("Handling SIGTERM. Exiting.")
    raise SystemExit

# Configure uncaught exception handling for threads.


def log_thread_exceptions(args):
    """Log uncaught exceptions for threads."""
    logging.critical("Uncaught exception", exc_info=(
        args.exc_type, args.exc_value, args.exc_traceback))


threading.excepthook = log_thread_exceptions


def log_exceptions(exc_type, exc_value, exc_traceback):
    """Log uncaught exceptions for main thread."""
    logging.critical("Uncaught exception", exc_info=(
        exc_type, exc_value, exc_traceback))


def configure_main_thread_exception_logging():
    sys.excepthook = log_exceptions


if __name__ == '__main__':
    """Quick test and demonstrate functionality."""
    logging.basicConfig(level=logging.INFO)

    sunrise_monitor = SunriseMonitor(print)
    sunrise_monitor.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    sunrise_monitor.stop()
