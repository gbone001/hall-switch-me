import os
import asyncio
from dotenv import load_dotenv
import discord
from discord import app_commands
from api_client import APIClient
import json
from collections import deque
import logging
from logging.handlers import TimedRotatingFileHandler
import gzip
import shutil
from typing import Optional, Tuple, List, Callable, Awaitable, Any

# ---------------------------------------------------------------------
# .env laden
# ---------------------------------------------------------------------
load_dotenv()

# Discord / App-Konfiguration
TOKEN = os.getenv('DISCORD_BOT_TOKEN', '')
ALLOWED_CHANNEL_ID = os.getenv('ALLOWED_CHANNEL_ID', '')
LANGUAGE = os.getenv('LANGUAGE', 'en')
COMMAND_SWITCH = os.getenv('COMMAND_SWITCH', 'switch')
COMMAND_LIST_PLAYERS = os.getenv('COMMAND_PLAYERS', 'players')

# RCON-Konfiguration
# Gemeinsamer Token für alle RCONs
API_TOKEN = os.getenv('API_TOKEN', '').strip()

# Variante A (empfohlen bei gleichem Token): Kommagetrennte Base-URLs oder JSON-Array von Strings
API_BASE_URLS = os.getenv('API_BASE_URLS', '').strip()

# Variante B (Legacy/Fallback): Einzel-Base-URL
API_BASE_URL = os.getenv('API_BASE_URL', '').strip()

# Variante C (Legacy/Optional): JSON-Array aus Objekten [{name, base_url, api_token}]
# – wird nur verwendet, wenn gesetzt; sonst ignoriert
RCONS_ENV = os.getenv('RCONS', '').strip()

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger('discord_bot')
logger.setLevel(logging.DEBUG)

handler = TimedRotatingFileHandler(
    filename='logs/discord_bot.log',
    when='midnight',
    interval=1,
    backupCount=7,
    encoding='utf-8',
)
handler.suffix = "%Y%m%d"
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

def compress_old_logs():
    for filename in os.listdir('logs'):
        if filename.startswith('discord_bot.log.') and not filename.endswith('.gz'):
            filepath = os.path.join('logs', filename)
            gz_path = filepath + '.gz'
            try:
                with open(filepath, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                os.remove(filepath)
            except Exception as e:
                logger.warning(f"Konnte {filepath} nicht komprimieren: {e}")

compress_old_logs()

# ---------------------------------------------------------------------
# Translations
# ---------------------------------------------------------------------
with open('translations.json', 'r', encoding='utf-8') as file:
    all_langs = json.load(file)
    lang = all_langs.get(LANGUAGE, all_langs['en'])

# ---------------------------------------------------------------------
# Discord-Intents
# ---------------------------------------------------------------------
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

# ---------------------------------------------------------------------
# Global players cache + queue (player_id = Steam64)
# ---------------------------------------------------------------------
switch_queue = deque()
player_list_cache = {'entries': {}}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _extract_players_map(players_response: dict) -> dict:
    if not isinstance(players_response, dict):
        return {}
    result = players_response.get('result', {})
    if not isinstance(result, dict):
        return {}
    players = result.get('players', {})
    return players if isinstance(players, dict) else {}

def _find_player_by_id_or_name(players_map: dict, player_id: str, player_name: Optional[str] = None) -> Tuple[Optional[str], Optional[dict]]:
    if not isinstance(players_map, dict):
        return None, None

    if player_id in players_map:
        return player_id, players_map[player_id]

    for pid, pdata in players_map.items():
        if not isinstance(pdata, dict):
            continue
        for k in ('steam_id_64', 'player_id', 'id'):
            if str(pdata.get(k, '')).strip() == str(player_id).strip():
                return pid, pdata

    if player_name:
        low = str(player_name).strip().lower()
        for pid, pdata in players_map.items():
            if isinstance(pdata, dict) and str(pdata.get('name', '')).strip().lower() == low:
                return pid, pdata

    return None, None


def _normalize_team(team_value: str) -> str:
    normalized = str(team_value or '').strip().lower()
    if normalized.startswith('axis'):
        return 'axis'
    if normalized.startswith('all') or normalized.startswith('ally'):
        return 'allies'
    return normalized


def _format_player_display_name(pdata: dict) -> str:
    if not isinstance(pdata, dict):
        return 'unknown'
    for key in ('name', 'player_name', 'nickname', 'personaname', 'player_id', 'steam_id_64', 'id'):
        value = pdata.get(key)
        if value:
            text = str(value).strip()
            if text:
                return text
    return 'unknown'


def _resolve_player_id_from_pdata(pdata: dict) -> Optional[str]:
    if not isinstance(pdata, dict):
        return None
    for key in ('steam_id_64', 'player_id', 'id', 'steam_id'):
        value = pdata.get(key)
        if value:
            candidate = str(value).strip()
            if candidate:
                return candidate
    return None


async def _attempt_switch_in_rcon(
    client: 'MyBot',
    send_func: Callable[[str], Awaitable[Any]],
    rcon_client: APIClient,
    player_id: str,
    player_name: str,
    player_team: str,
    requester_discord_id: str,
) -> None:
    if not player_team:
        await send_func(lang['player_not_in_game'])
        logger.info(f'{player_name}: kein Team in Daten.')
        return

    target_team = 'axis' if player_team == 'allies' else 'allies'
    gamestate = await client._get_gamestate_async(rcon_client)
    res = gamestate.get('result', {}) if isinstance(gamestate, dict) else {}
    num_allied_players = int(res.get('num_allied_players', 0))
    num_axis_players = int(res.get('num_axis_players', 0))
    target_team_players = num_axis_players if target_team == 'axis' else num_allied_players

    if target_team_players < 50:
        response = await client._switch_player_now_async(rcon_client, player_id)
        if response.get('result') is True and not response.get('failed'):
            await send_func(lang['switch_request_success'].format(player_name=player_name))
            logger.info(f'Switch OK: {player_name}')
        else:
            await send_func(lang['switch_request_failure'].format(player_name=player_name))
            logger.warning(f'Switch FAIL: {player_name}')
    else:
        MAX_QUEUE_SIZE = 10
        if len(switch_queue) >= MAX_QUEUE_SIZE:
            await send_func(lang['queue_full'])
            logger.info('Warteschlange voll.')
        else:
            switch_queue.append({
                'player_id': player_id,
                'player_name': player_name,
                'target_team': target_team,
                'discord_id': requester_discord_id,
            })
            await send_func(lang['added_to_queue'].format(
                player_name=player_name,
                target_team=target_team.capitalize()
            ))
            logger.info(f'In Queue: {player_name} -> {target_team}')


async def _handle_players_list(
    client: 'MyBot',
    send_func: Callable[[str], Awaitable[Any]],
    filter_arg: str,
) -> None:
    if filter_arg in ('axis', 'allies'):
        team_filter = filter_arg
        team_label = lang.get(f'team_{team_filter}', team_filter.capitalize())
    elif not filter_arg or filter_arg in ('all', 'both'):
        team_filter = None
        team_label = lang.get('team_all', 'All teams')
    else:
        await send_func(lang.get(
            'invalid_team_filter',
            'Filter must be axis or allies (e.g., !{COMMAND_LIST_PLAYERS} axis).'
        ).format(COMMAND_LIST_PLAYERS=COMMAND_LIST_PLAYERS))
        return

    if not client.api_clients:
        await send_func(lang.get('rcon_not_configured', 'RCON is not configured.'))
        return

    player_list_cache['entries'] = {}
    current_index = 0

    for api_client in client.api_clients:
        server_name = getattr(api_client, '_rcon_name', 'unknown')
        try:
            players_response = await client._get_detailed_players_async(api_client)
        except Exception as exc:
            await send_func(lang.get(
                'players_list_failure',
                'Failed to fetch players from {server_name}: {error}'
            ).format(server_name=server_name, error=str(exc)))
            continue

        players_map = _extract_players_map(players_response)
        filtered_players = []
        for pdata in players_map.values():
            if not isinstance(pdata, dict):
                continue
            pdata_team = _normalize_team(pdata.get('team', ''))
            if team_filter and pdata_team != team_filter:
                continue
            player_id = _resolve_player_id_from_pdata(pdata) or ''
            display_name = _format_player_display_name(pdata)
            if not display_name:
                continue

            current_index += 1
            player_list_cache['entries'][current_index] = {
                'api_client': api_client,
                'player_id': player_id,
                'player_name': display_name,
                'team': pdata_team,
                'server_name': server_name,
            }
            filtered_players.append((current_index, display_name))

        header = lang.get(
            'players_list_header',
            '{server_name} – {team_label} ({count} players)'
        ).format(server_name=server_name, team_label=team_label, count=len(filtered_players))

        if filtered_players:
            lines = [header] + [f"{idx}. {name}" for idx, name in filtered_players]
        else:
            lines = [
                header,
                lang.get(
                    'players_list_empty',
                    'No players on {server_name} ({team_label}).'
                ).format(server_name=server_name, team_label=team_label)
            ]
        await send_func('\n'.join(lines))

# ---------------------------------------------------------------------
# Bot-Klasse
# ---------------------------------------------------------------------
class MyBot(discord.Client):
    def __init__(self, intents):
        super().__init__(intents=intents)
        self.api_clients: List[APIClient] = self._load_rcons()
        self.tree = app_commands.CommandTree(self)
        logger.debug(f'Bot-Instanz initialisiert. RCON-Clients: {len(self.api_clients)}')

    def _load_rcons(self) -> List[APIClient]:
        clients: List[APIClient] = []

        # 1) RCONS (Objektliste) – nur falls gesetzt (Legacy/Optional)
        if RCONS_ENV:
            try:
                parsed = json.loads(RCONS_ENV)
                if isinstance(parsed, list):
                    for idx, item in enumerate(parsed):
                        if not isinstance(item, dict):
                            logger.warning(f"RCONS[{idx}] ist kein Objekt; wird übersprungen.")
                            continue
                        name = str(item.get('name', f'RCON{idx}'))
                        base_url = str(item.get('base_url', '')).rstrip('/')
                        token = str(item.get('api_token', API_TOKEN)).strip()
                        if not base_url or not token:
                            logger.warning(f"RCONS[{idx}] unvollständig (base_url/api_token fehlen); übersprungen.")
                            continue
                        c = APIClient(base_url, token)
                        setattr(c, '_rcon_name', name)
                        clients.append(c)
                else:
                    logger.error("RCONS ist gesetzt, aber kein JSON-Array.")
            except Exception as e:
                logger.error(f"Fehler beim Parsen von RCONS: {e}")

        # 2) API_BASE_URLS (empfohlen) – gleiche Tokens, unterschiedliche Base-URLs
        if not clients and API_BASE_URLS:
            urls: List[str] = []
            # JSON-Array?
            if API_BASE_URLS.startswith('['):
                try:
                    parsed = json.loads(API_BASE_URLS)
                    if isinstance(parsed, list):
                        urls = [str(u).strip().rstrip('/') for u in parsed if str(u).strip()]
                except Exception as e:
                    logger.error(f"Fehler beim Parsen von API_BASE_URLS (JSON): {e}")
            # Kommagetrennte Liste
            if not urls:
                urls = [u.strip().rstrip('/') for u in API_BASE_URLS.split(',') if u.strip()]

            for i, base_url in enumerate(urls):
                if not base_url:
                    continue
                if not API_TOKEN:
                    logger.error("API_TOKEN fehlt; kann Client nicht bauen.")
                    continue
                c = APIClient(base_url, API_TOKEN)
                setattr(c, '_rcon_name', f'RCON{i+1}')
                clients.append(c)

        # 3) Fallback: Single-URL
        if not clients and API_BASE_URL:
            if not API_TOKEN:
                logger.error("API_TOKEN fehlt; Single-RCON kann nicht erzeugt werden.")
            else:
                c = APIClient(API_BASE_URL.rstrip('/'), API_TOKEN)
                setattr(c, '_rcon_name', 'default')
                clients.append(c)

        if not clients:
            logger.error("Keine RCON-Konfiguration gefunden. Bitte .env prüfen.")
        return clients

    @app_commands.command(name='players')
    @app_commands.describe(team='Filter axis or allies')
    @app_commands.choices(team=[
        app_commands.Choice(name='All', value='all'),
        app_commands.Choice(name='Axis', value='axis'),
        app_commands.Choice(name='Allies', value='allies'),
    ])
    async def players_command(
        self,
        interaction: discord.Interaction,
        team: app_commands.Choice[str] | None = None,
    ):
        filter_arg = team.value if team else ''
        await interaction.response.defer()

        async def send_block(text: str):
            await interaction.followup.send(content=text)

        await _handle_players_list(self, send_block, filter_arg)

    @app_commands.command(name='switch')
    @app_commands.describe(player_number='Number from the latest /players response')
    async def switch_command(
        self,
        interaction: discord.Interaction,
        player_number: int,
    ):
        await interaction.response.defer()

        entry = player_list_cache['entries'].get(player_number)
        if not entry:
            await interaction.followup.send(lang['players_list_missing_number'].format(
                number=player_number,
                COMMAND_LIST_PLAYERS=COMMAND_LIST_PLAYERS,
            ))
            return

        player_id = entry.get('player_id')
        if not player_id:
            await interaction.followup.send(lang['players_list_missing_id'].format(
                number=player_number
            ))
            return

        async def send_block(text: str):
            await interaction.followup.send(content=text)

        await _attempt_switch_in_rcon(
            self,
            send_block,
            entry['api_client'],
            player_id,
            entry.get('player_name', player_id),
            entry.get('team', ''),
            str(interaction.user.id),
        )

    async def setup_hook(self):
        self.loop.create_task(self.process_switch_queue())
        self.tree.add_command(self.players_command)
        self.tree.add_command(self.switch_command)
        await self.tree.sync()

    async def on_ready(self):
        logger.info(lang['logged_in'].format(bot_name=self.user))
        logger.info(lang.get('api_initialized', 'API initialized.'))
        await self._sync_commands_for_allowed_channel()

    async def on_message(self, message: discord.Message):
        try:
            if message.author.bot or message.channel.id != int(ALLOWED_CHANNEL_ID):
                return
        except Exception:
            return

        logger.debug(f'Nachricht empfangen von {message.author}: {message.content}')
        await handle_command(self, message)

    # ---------------------- Async-Wrapper für APIClient ----------------------
    async def _get_detailed_players_async(self, client: APIClient) -> dict:
        return await asyncio.to_thread(client.get_detailed_players)

    async def _get_gamestate_async(self, client: APIClient) -> dict:
        return await asyncio.to_thread(client.get_gamestate)

    async def _switch_player_now_async(self, client: APIClient, player_id: str) -> dict:
        return await asyncio.to_thread(client.switch_player_now, player_id)

    async def _find_player_across_rcons(
        self, player_id: str, player_name: Optional[str] = None
    ) -> Tuple[Optional[APIClient], Optional[str], Optional[dict]]:
        """
        Sucht den Spieler über alle konfigurierten RCONs.
        Rückgabe: (client, found_id, pdata) oder (None, None, None)
        """
        for client in self.api_clients:
            try:
                players_resp = await self._get_detailed_players_async(client)
                players_map = _extract_players_map(players_resp)
                found_id, pdata = _find_player_by_id_or_name(players_map, player_id, player_name)
                if found_id:
                    rname = getattr(client, '_rcon_name', '?')
                    logger.debug(f"Spieler {player_name or player_id} gefunden auf RCON '{rname}'")
                    return client, found_id, pdata
            except Exception as e:
                rname = getattr(client, '_rcon_name', '?')
                logger.warning(f"Fehler bei get_detailed_players auf RCON '{rname}': {e}")
        return None, None, None

    # ---------------------- Queue-Verarbeitung ------------------------
    async def process_switch_queue(self):
        await self.wait_until_ready()
        try:
            channel = self.get_channel(int(ALLOWED_CHANNEL_ID))
        except Exception:
            channel = None

        while not self.is_closed():
            if switch_queue:
                item = switch_queue[0]
                player_id = item['player_id']
                player_name = item.get('player_name')
                target_team = item['target_team']

                logger.debug(f'Queue: {player_name or player_id} -> Zielteam {target_team}')

                try:
                    client, found_id, pdata = await self._find_player_across_rcons(player_id, player_name)
                    if not client or not found_id or not isinstance(pdata, dict):
                        if channel:
                            await channel.send(lang['player_left_game'].format(
                                player_name=player_name or player_id
                            ))
                        logger.info(f'{player_name or player_id}: nicht (mehr) im Spiel.')
                        switch_queue.popleft()
                        await asyncio.sleep(1)
                        continue

                    gamestate = await self._get_gamestate_async(client)
                    res = gamestate.get('result', {}) if isinstance(gamestate, dict) else {}
                    num_allied_players = int(res.get('num_allied_players', 0))
                    num_axis_players = int(res.get('num_axis_players', 0))
                    target_team_players = num_axis_players if target_team == 'axis' else num_allied_players

                    if target_team_players < 50:
                        response = await self._switch_player_now_async(client, player_id)
                        if response.get('result') is True and not response.get('failed'):
                            if channel:
                                await channel.send(lang['switch_request_success'].format(
                                    player_name=player_name or player_id
                                ))
                            logger.info(f'Switch OK: {player_name or player_id}')
                        else:
                            if channel:
                                await channel.send(lang['switch_request_failure'].format(
                                    player_name=player_name or player_id
                                ))
                            logger.warning(f'Switch FAIL: {player_name or player_id}')
                        switch_queue.popleft()
                    else:
                        logger.debug(f"Zielteam voll für {player_name or player_id}.")
                except Exception as e:
                    logger.error(f"Fehler in process_switch_queue: {e}")
                    switch_queue.popleft()
            await asyncio.sleep(10)

    async def _sync_commands_for_guild(self, guild) -> None:
        if not guild:
            return
        guild_id = getattr(guild, 'id', None)
        try:
            await self.tree.sync(guild=guild)
            logger.info(f'Synced slash commands for guild {guild_id}.')
        except Exception as exc:
            logger.warning(f'Failed to sync slash commands for guild {guild_id}: {exc}')

    async def _sync_commands_for_allowed_channel(self) -> None:
        if not ALLOWED_CHANNEL_ID:
            return
        try:
            channel_id = int(ALLOWED_CHANNEL_ID)
        except ValueError:
            logger.warning('ALLOWED_CHANNEL_ID is invalid; skipping slash-command sync for the guild.')
            return

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception as exc:
                logger.debug(f'Failed fetching allowed channel for command sync: {exc}')
                return

        await self._sync_commands_for_guild(getattr(channel, 'guild', None))

# ---------------------------------------------------------------------
# Command-Handler
# ---------------------------------------------------------------------
async def handle_command(client: MyBot, message: discord.Message):
    content = message.content.strip()

    if content.startswith(f'!{COMMAND_SWITCH}'):
        parts = content.split()
        if len(parts) == 2 and parts[1].isdigit():
            number = int(parts[1])
            entry = player_list_cache['entries'].get(number)
            if not entry:
                await message.channel.send(lang['players_list_missing_number'].format(
                    number=number,
                    COMMAND_LIST_PLAYERS=COMMAND_LIST_PLAYERS,
                ))
                return

            player_id = entry.get('player_id')
            if not player_id:
                await message.channel.send(lang['players_list_missing_id'].format(number=number))
                return

            await _attempt_switch_in_rcon(
                client,
                message.channel.send,
                entry['api_client'],
                player_id,
                entry.get('player_name', player_id),
                entry.get('team', ''),
                str(message.author.id),
            )
        else:
            await message.channel.send(lang['switch_invalid_usage'].format(
                COMMAND_SWITCH=COMMAND_SWITCH,
                COMMAND_LIST_PLAYERS=COMMAND_LIST_PLAYERS,
            ))
            logger.warning(f'Unbekannter Befehl/Parameter: {message.author}: {message.content}')

    elif content.startswith(f'!{COMMAND_LIST_PLAYERS}'):
        parts = content.split()
        filter_arg = parts[1].lower() if len(parts) > 1 else ''
        await _handle_players_list(client, message.channel.send, filter_arg)

    else:
        await message.channel.send(lang['unknown_command'])
        logger.warning(f'Unbekannter Befehl: {message.author}: {message.content}')
# ---------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------
bot = MyBot(intents=intents)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandSignatureMismatch):
        logger.warning('Slash command signature mismatch detected; syncing commands.')
        guild = interaction.guild
        if guild:
            await bot._sync_commands_for_guild(guild)
        else:
            try:
                await bot.tree.sync()
            except Exception as exc:
                logger.warning(f'Failed to sync slash commands after signature mismatch: {exc}')
        if not interaction.response.is_done():
            try:
                await interaction.response.send_message(
                    lang.get(
                        'command_signature_updated',
                        'Slash commands were refreshed; please try again.'
                    ),
                    ephemeral=True,
                )
            except Exception as delivery_exc:
                logger.debug(f'Could not notify user about command sync: {delivery_exc}')
        return

    raise error

bot.run(TOKEN)
