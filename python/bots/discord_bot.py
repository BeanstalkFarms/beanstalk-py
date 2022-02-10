import abc
from enum import Enum
from google.cloud import storage
import google.api_core.exceptions as google_api_exceptions
import json
import logging
import logging.handlers
import os
import signal
import subprocess

import discord
from discord.ext import tasks, commands

from bots import util
from data_access.eth_chain import is_valid_wallet_address


DISCORD_CHANNEL_ID_PEG_CROSSES = 911338190198169710
DISCORD_CHANNEL_ID_SEASONS = 911338078080221215
DISCORD_CHANNEL_ID_POOL = 915372733758603284
DISCORD_CHANNEL_ID_BEANSTALK = 918240659914227713
DISCORD_CHANNEL_ID_MARKET = 940729085095723069
DISCORD_CHANNEL_ID_TEST_BOT = 908035718859874374
BUCKET_NAME = 'beanstalk_bots_data'
PROD_BLOB_NAME = 'prod_channel_to_wallets'
STAGE_BLOB_NAME = 'stage_channel_to_wallets'
WALLET_WATCH_LIMIT = 10

class Channel(Enum):
    PEG = 0
    SEASONS = 1
    POOL = 2
    BEANSTALK = 3
    MARKET = 4

class DiscordClient(discord.ext.commands.Bot):

    def __init__(self, prod=False):
        super().__init__(command_prefix=commands.when_mentioned_or("!"))
        self.add_cog(WalletMonitoring(self))
        configure_bot_commands(self)
        
        # Retrieve bucket.
        bucket = self.retrieve_or_init_bucket()

        if prod:
            self.wallets_blob = bucket.blob(PROD_BLOB_NAME)
            self._chat_id_peg = DISCORD_CHANNEL_ID_PEG_CROSSES
            self._chat_id_seasons = DISCORD_CHANNEL_ID_SEASONS
            self._chat_id_pool = DISCORD_CHANNEL_ID_POOL
            self._chat_id_beanstalk = DISCORD_CHANNEL_ID_BEANSTALK
            self._chat_id_market = DISCORD_CHANNEL_ID_MARKET
            logging.info('Configured as a production instance.')
        else:
            self.wallets_blob = bucket.blob(STAGE_BLOB_NAME)
            self._chat_id_peg = DISCORD_CHANNEL_ID_TEST_BOT
            self._chat_id_seasons = DISCORD_CHANNEL_ID_TEST_BOT
            self._chat_id_pool = DISCORD_CHANNEL_ID_TEST_BOT
            self._chat_id_beanstalk = DISCORD_CHANNEL_ID_TEST_BOT
            self._chat_id_market = DISCORD_CHANNEL_ID_TEST_BOT
            logging.info('Configured as a staging instance.')

        # Load wallet map from source. Map may be modified by this thread only (via discord.py lib).
        # Sets self.channel_to_wallets.
        if not self.download_channel_to_wallets():
            logging.critical('Failed to download wallet data. Exiting...')
            exit(1)
        self.channel_id_to_channel = {}

        self.msg_queue = []
        self.status_text = ''

        # Because this changes the bot name, prod vs stage is handled differently. This prevents
        # the publicly visible BeanBot from having its name changed.
        if not prod:
            self.price_monitor = util.PreviewMonitor(
                self.set_nickname_price, self.set_status)
            self.price_monitor.start()

        self.peg_cross_monitor = util.PegCrossMonitor(
            self.send_msg_peg, prod=prod)
        self.peg_cross_monitor.start()

        self.sunrise_monitor = util.SunriseMonitor(
            self.send_msg_seasons, channel_to_wallets=self.channel_to_wallets, prod=prod)
        self.sunrise_monitor.start()

        self.uniswap_pool_monitor = util.UniswapPoolMonitor(self.send_msg_pool, prod=prod)
        self.uniswap_pool_monitor.start()

        self.curve_pool_monitor = util.CurvePoolMonitor(self.send_msg_pool, prod=prod)
        self.curve_pool_monitor.start()

        self.beanstalk_monitor = util.BeanstalkMonitor(self.send_msg_beanstalk, prod=prod)
        self.beanstalk_monitor.start()

        self.market_monitor = util.MarketMonitor(self.send_msg_market, prod=prod)
        self.market_monitor.start()

        # Ignore exceptions of this type and retry. Note that no logs will be generated.
        self.send_queued_messages.add_exception_type(discord.errors.DiscordServerError)
        # Start the message queue sending task in the background.
        self.send_queued_messages.start()

        # Ignore exceptions of this type and retry. Note that no logs will be generated.
        self.set_presence.add_exception_type(discord.errors.DiscordServerError)
        # Start the price display task in the background.
        self.set_presence.start()

    def stop(self):
        self.upload_channel_to_wallets()
        self.price_monitor.stop()
        self.peg_cross_monitor.stop()
        self.sunrise_monitor.stop()
        self.uniswap_pool_monitor.stop()
        self.curve_pool_monitor.stop()
        self.beanstalk_monitor.stop()
        self.market_monitor.stop()

    # NOTE(funderberker): This bot does not have permissions to change its nickname. This will
    # silently do nothing.
    def set_nickname_price(self, text):
        """Set bot server nickname price."""
        self.nickname = f'(stage) BEAN: {text}'

    def set_status(self, text):
        """Set bot custom status text."""
        self.status_text = text

    def send_msg_peg(self, text):
        """Send a message through the Discord bot in the peg channel."""
        self.msg_queue.append((Channel.PEG, text))

    def send_msg_seasons(self, text, channel_id=Channel.SEASONS):
        """Send a message through the Discord bot in the seasons channel."""
        self.msg_queue.append((channel_id, text))

    def send_msg_pool(self, text):
        """Send a message through the Discord bot in the pool channel."""
        self.msg_queue.append((Channel.POOL, text))

    def send_msg_beanstalk(self, text):
        """Send a message through the Discord bot in the beanstalk channel."""
        self.msg_queue.append((Channel.BEANSTALK, text))

    def send_msg_market(self, text):
        """Send a message through the Discord bot in the market channel."""
        self.msg_queue.append((Channel.MARKET, text))

    async def on_ready(self):
        self._channel_peg = self.get_channel(self._chat_id_peg)
        self._channel_seasons = self.get_channel(
            self._chat_id_seasons)
        self._channel_pool = self.get_channel(self._chat_id_pool)
        self._channel_beanstalk = self.get_channel(self._chat_id_beanstalk)
        self._channel_market = self.get_channel(self._chat_id_market)

        # Init DM channels.
        for channel_id in self.channel_to_wallets.keys():
            self.channel_id_to_channel[channel_id] = await self.fetch_channel(channel_id)
        
        logging.info(
            f'Discord channels are {self._channel_peg}, {self._channel_seasons}, '
            f'{self._channel_pool}, {self._channel_beanstalk}, {self._channel_market}')
        # Log the commit of this run.
        logging.info('Git commit is ' + subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=os.path.dirname(os.path.realpath(__file__))
            ).decode('ascii').strip())

    async def send_dm(self, channel_id, text):
        logging.warning(channel_id)
        try:
            # channel = await self.fetch_channel(channel_id)
            # channel = self.get_channel(int(channel_id))
            channel = self.channel_id_to_channel[channel_id]
            await channel.send(text)
        except AttributeError as e:
            logging.error('Failed to send DM')
            logging.exception(e)

    @tasks.loop(seconds=0.1, reconnect=True)
    async def set_presence(self):
        if self.status_text:
            await self.change_presence(activity=discord.Activity(type=discord.ActivityType.playing,
                                                                 name=self.status_text))
            logging.info(f'Bot status changed to {self.status_text}')
            self.status_text = ''

    @set_presence.before_loop
    async def before_set_presence_loop(self):
        """Wait until the bot logs in."""
        await self.wait_until_ready()

    @tasks.loop(seconds=0.1, reconnect=True)
    async def send_queued_messages(self):
        """Send messages in queue.

        Reconnect logic applies to Loop._valid_exception (OSError, discord.GatewayNotFound,
        discord.ConnectionClosed, aiohttp.ClientError, asyncio.TimeoutError).
        
        We handle unexpected exceptions in this method so that:
        1. We can log them usefully.
        2. This thread does not crash forever.
        The Discord.py lib allows us to ignore an exception and restart, or die on the exception. 
        We want to log it _and_ not die.
        """
        try:
            for channel, msg in self.msg_queue:
                # Ignore empty messages.
                if not msg:
                    pass
                elif channel is Channel.PEG:
                    await self._channel_peg.send(msg)
                elif channel is Channel.SEASONS:
                    await self._channel_seasons.send(msg)
                elif channel is Channel.POOL:
                    await self._channel_pool.send(msg)
                elif channel is Channel.BEANSTALK:
                    await self._channel_beanstalk.send(msg)
                elif channel is Channel.MARKET:
                    await self._channel_market.send(msg)
                # If channel is a channel_id string.
                elif type(channel) == str:
                    await self.send_dm(channel, msg)
                else:
                    logging.error('Unknown channel seen in msg queue: {channel}')
                self.msg_queue = self.msg_queue[1:]
                logging.info(f'Message sent through {channel} channel:\n{msg}\n')
        except Exception as e:
            logging.error('Failed to send message to Discord server. Will retry.')
            logging.exception(e)
            return

    @send_queued_messages.before_loop
    async def before_send_queued_messages_loop(self):
        """Wait until the bot logs in."""
        await self.wait_until_ready()

    async def on_message(self, message):
        """Respond to messages."""
        # Do not reply to itself.
        if message.author.id == self.user.id:
            return

        # Process commands.
        await self.process_commands(message)

    def retrieve_or_init_bucket(self):
        """Get the Google Cloud Storage bucket, or create a new one with pertinent files."""
        storage_client = storage.Client()
        bucket = storage_client.lookup_bucket(BUCKET_NAME) 
        
        # If no bucket, this bucket did not exist and a new empty bucket has been created.
        # This could indicate a very bad data loss event has occurred if it is not the first run.
        if bucket is None:
            logging.critical('Existing bucket not detected. New bucket created. '
                             'This is likely a data loss event.')
            # Initialize new bucket.
            bucket = storage_client.create_bucket(BUCKET_NAME, location='US-CENTRAL1')
        
        return bucket

    def add_to_watched_addresses(self, address, channel_id):
        try:
            wallets = self.channel_to_wallets[channel_id]
        except KeyError:
            # If nothing is being watched for this channel_id, initialize list.
            wallets = []
            self.channel_to_wallets[channel_id] = wallets
        
        # If this address is already being watched in this channel, do nothing.
        if address in wallets:
            return
        
        # Append the address to the existing watch list.
        wallets.append(address)
        logging.info(f'Discord channel {channel_id} is now watching {address}')

        # Update cloud source of truth with new data.
        self.upload_channel_to_wallets()

    def remove_from_watched_addresses(self, address, channel_id):
        try:
            wallets = self.channel_to_wallets[channel_id]
        except KeyError:
            # If nothing is being watched for this channel_id, then nothing to remove.
            return
        
        # If this address not already being watched in this channel, do nothing.
        if address not in wallets:
            return
        
        # Remove the address from the existing watch list.
        wallets.remove(address)
        logging.info(f'Discord channel {channel_id} is no longer watching {address}')

        # Update cloud source of truth with new data.
        self.upload_channel_to_wallets()

    def download_channel_to_wallets(self):
        """Pull down data from cloud source of truth. Returns True/False based on success."""
        try:
            map_str = self.wallets_blob.download_as_string(timeout=120).decode('utf8')
            logging.info(f'Channel to wallets map string pulled from cloud source:\n{map_str}')
            self.channel_to_wallets = json.loads(map_str)
        except google_api_exceptions.NotFound as e:
            # Data blob not found. Confirm it does not exist, then init to empty file.
            if not self.wallets_blob.exists():
                logging.critical(f'Blob file {self.wallets_blob.name} does not exist. May be a '
                                 'data loss event. Creating empty blob file.')
                self.channel_to_wallets = {}
                self.upload_channel_to_wallets()
            else:
                logging.exception(e)
                return False
        except Exception as e:
            logging.error('Failed to download wallet watching list.')
            logging.exception(e)
            return False
        logging.info('Successfully downloaded channel_to_wallets map.')
        return True


    def upload_channel_to_wallets(self):
        """Update cloud source of truth with new data. Returns True/False based on success."""
        try:
            self.wallets_blob.upload_from_string(json.dumps(self.channel_to_wallets), num_retries=3, timeout=20)
        except Exception as e:
            logging.error('Failed to upload wallet watching changes to cloud. Will attempt again ' \
                          'on next change.')
            logging.exception(e)
            return False
        logging.info('Successfully uploaded channel_to_wallets map.')
        return True

    @abc.abstractmethod
    def isDM(self, channel):
        """Return True is this context is from a dm, else False."""
        return isinstance(channel, discord.channel.DMChannel)

def configure_bot_commands(bot):
    """Define all bot chat commands that users can utilize.
    
    Due to quirks of the discord.py lib, these commands must be defined outside of the bot class
    definition.
    """
    @bot.command(pass_context=True)
    async def botstatus(ctx):
        """Check if bot is currently running."""
        await ctx.send('I am alive and running!')

class WalletMonitoring(commands.Cog):
    """Wallet watching commands."""
    def __init__(self, bot):
        self.bot = bot

    @commands.command(pass_context=True)
    @commands.dm_only()
    async def list(self, ctx):
        """List all addresses you are currently watching."""
        logging.warning(f'list request from channel id == {channel_id(ctx)}')
        watched_addrs = self.bot.channel_to_wallets.get(channel_id(ctx)) or []
        addr_list_str = ', '.join([f'`{addr}`' for addr in watched_addrs])
        if addr_list_str:
            await ctx.send(f'Wallets you are watching:\n{addr_list_str}')
        else:
            await ctx.send(f'You are not currently watching any wallets.')


    # This is challenging because the synchronous subgraph access cannot be called by the
    # discord coroutines. Would we even want to allow random users to use our subgraph key to
    # make requests on demand?
    # @commands.command(pass_context=True)
    # @commands.dm_only()
    # async def status(self, ctx):
    #     """Get Beanstalk status of watched addresses."""      
    #     # Check if no addresses are being watched.
    #     dm_id = channel_id(ctx)
    #     watched_addrs = self.bot.channel_to_wallets.get(dm_id)
    #     if not watched_addrs:
    #         await ctx.send(f'You are not currently watching any addresses.')
    #         return
    #     await ctx.send(self.bot.sunrise_monitor.wallets_str(self.bot.channel_to_wallets[dm_id]))


    @commands.command(pass_context=True)
    @commands.dm_only()
    async def add(self, ctx, *, address=None):
        """Get seasonal updates about the Beanstalk status of an address."""
        if address is None:
            await ctx.send(f'You must provide a wallet address to add. No change to watch list.')
            return

        # Address must be a valid ETH address.
        if not is_valid_wallet_address(address):
            await ctx.send(f'Invalid address provided (`{address}`). No change to watch list.')
            return

        # Limit user to 5 watched wallets. This prevents abuse/spam.
        watched_addrs = self.bot.channel_to_wallets.get(channel_id(ctx)) or []
        if len(watched_addrs) >= WALLET_WATCH_LIMIT:
            await ctx.send(f'Each user may only monitor up to {WALLET_WATCH_LIMIT} wallets. '
                           'No change to watch list.')
            return
        
        # Check if address is already being watched.
        if address in watched_addrs:
            await ctx.send(f'You are already watching `{address}`. No change to watch list.')
            return
        
        # Append the address to the list of watched addresses.
        self.bot.add_to_watched_addresses(address, channel_id(ctx))

        # If DM channel is new, cache the channel.
        if channel_id(ctx) not in self.bot.channel_id_to_channel:
            self.bot.channel_id_to_channel[channel_id(ctx)] = ctx.channel

        await ctx.send(f'You are now watching `{address}` in this DM conversation.')


    @commands.command(pass_context=True)
    @commands.dm_only()
    async def remove(self, ctx, *, address=None):
        """Stop getting updates about the Beanstalk status of an address."""
        if address is None:
            await ctx.send(f'You must provide a wallet address to remove. No change to watch list.')
            return

        # Address must be a valid ETH address.
        if not is_valid_wallet_address(address):
            await ctx.send(f'Invalid address provided (`{address}`). No change to watch list.')
            return
        
        # Check if address is already being watched.
        watched_addrs = self.bot.channel_to_wallets.get(channel_id(ctx)) or []
        if address not in watched_addrs:
            await ctx.send(f'You are not already watching `{address}`. No change to watch list.')
            return
        
        self.bot.remove_from_watched_addresses(address, channel_id(ctx))
        await ctx.send(f'You are no longer watching `{address}` in this DM conversation.')

def channel_id(ctx):
    """Returns a string representation of the channel id from a context object."""
    return str(ctx.channel.id)

if __name__ == '__main__':
    logging.basicConfig(format='Discord Bot : %(levelname)s : %(asctime)s : %(message)s',
                        level=logging.INFO, handlers=[
                            logging.handlers.RotatingFileHandler(
                                "discord_bot.log", maxBytes=util.FIVE_HUNDRED_MEGABYTES),
                            logging.StreamHandler()])
    signal.signal(signal.SIGTERM, util.handle_sigterm)

    util.configure_main_thread_exception_logging()

    # Automatically detect if this is a production environment.
    try:
        token = os.environ["DISCORD_BOT_TOKEN_PROD"]
        prod = True
    except KeyError:
        token = os.environ["DISCORD_BOT_TOKEN"]
        prod = False

    discord_client = DiscordClient(prod=prod)

    try:
        discord_client.run(token)
    except (KeyboardInterrupt, SystemExit):
        pass
    # Note that discord bot cannot send shutting down messages in its channel, due to lib impl.
    discord_client.stop()
