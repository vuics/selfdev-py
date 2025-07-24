#!/usr/bin/env python
'''
Selfdev CLI

It is a standalone XMPP command-line client based on slixmpp,
which can be used from both Jupyter notebooks and the command line.

The script allows you to:
 * Connect to an XMPP server with credentials from environment variables or command-line arguments.
 * Send messages to a user or a room.
 * Receive messages and print them to stdout.
 * Exit after receiving a specified number of messages or after a timeout.
 * Handle errors properly.

Instructions for AI:

---

I attached an example of a client based on the slixmpp python library. Code a command line XMPP client based on slixmpp that can be used from jupyter notebooks or from command line to:
• connect to XMPP with host, user, password, muc_host, join_rooms, nick, etc. (specified in environment variables or in arguments);
• send a message to a user directly or to rooms (user, room, and message specified in argument);
• receive messages from users directly or from rooms and outputs them to stdout;
• exit by receiving a specified number of messages (specified in argument) or by timeout (specified in  argument);
• there is no need to reply to any received messages;
• output errors to stderr.

The command is to interact with deployed agents in the agentic system. In the basic case, the cli client will connect, send message to a user or a room, listen for an answer (or several), outputs the answers, and exits.
The command line tool should have arguments including --help with explanation of all the other arguments and --version (initial is v0.0.1).

The following env vars should work:
ALLOW_INSECURE
REGISTER_PORT

Produce only one selfdev_cli.py file that does not depend on the files that I attached. The functions and classes from selfdev_cli.py can be imported in a Jupyter Notebook to communicate with agents through XMPP programmatically. You can give me an example of code that I can copy-paste to Jupyter Notebooks to use selfdev_cli.py.

---

'''

import os
import sys
import asyncio
import argparse
import logging
import time
import ssl
from slixmpp import ClientXMPP
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("XMPPClient")

ALLOW_INSECURE = os.getenv("ALLOW_INSECURE", "False").lower() in ("true", "1", "yes")


class SelfdevClient(ClientXMPP):
  """An XMPP client that connects to a server, sends a message, and listens for responses."""

  def __init__(self, *, jid, password, recipient=None, message=None, rooms=None, nick=None,
               muc_host=None, max_messages=None, timeout=None, allow_insecure=False, verbose=False,
               output_format="[{nick}]: {body}\n"):
    super().__init__(jid, password)
    self.recipient = recipient
    self.message = message
    self.rooms = rooms or []
    self.nick = nick
    self.muc_host = muc_host
    self.max_messages = max_messages
    self.timeout = timeout
    self.received_messages = 0
    self.disconnect_event = asyncio.Event()
    self.output_format = output_format
    self.output = ""

    if verbose:
      logger.setLevel(logging.DEBUG)

    logger.debug("Initializing XMPP client")
    if allow_insecure:
      logger.debug("Allowing insecure SSL connections")
      self.ssl_context = ssl.create_default_context()
      self.ssl_context.check_hostname = False
      self.ssl_context.verify_mode = ssl.CERT_NONE
      self.add_event_handler('ssl_invalid_cert', self.ssl_invalid_cert)

    self.add_event_handler("session_start", self.start)
    self.add_event_handler("message", self.message_received)
    self.add_event_handler("groupchat_message", self.groupchat_message_received)
    self.add_event_handler("disconnected", self.handle_disconnection)

    self.register_plugin('xep_0030')  # Service Discovery
    self.register_plugin('xep_0045')  # Multi-User Chat
    self.register_plugin('xep_0199')  # XMPP Ping
    # self.register_plugin('xep_0004')  # Data Forms
    # self.register_plugin('xep_0060')  # PubSub
    # self.register_plugin('xep_0249')  # Direct MUC Invitations

  def ssl_invalid_cert(self, pem_cert):
    logger.warning("Warning: Invalid SSL certificate received")
    return True

  async def start(self, event):
    logger.debug("Session started, sending presence and getting roster")
    self.send_presence()
    await self.get_roster()

    # Join MUC rooms
    for room in self.rooms:
      room_jid = f"{room}@{self.muc_host}"
      logger.info(f"Joining room: {room_jid} as {self.nick}")
      try:
        self.plugin['xep_0045'].join_muc(room_jid, self.nick)
      except Exception as e:
        logger.error(f"Error joining MUC room {room_jid}: {e}")

    # Send a message if required
    if self.recipient and self.message:
      logger.info(f"Sending message to {self.recipient}: {self.message}")
      self.send_message(mto=self.recipient, mbody=self.message, mtype='chat')

    # Send a message to rooms if required
    if self.rooms and self.message:
      await asyncio.sleep(0.1)
      for room in self.rooms:
        room_jid = f"{room}@{self.muc_host}"
        logger.info(f"Sending message to room '{room_jid}': {self.message}")
        self.send_message(mto=room_jid, mbody=self.message, mtype='groupchat')

    if not self.max_messages and not self.timeout:
      logger.info("No message limit or timeout specified, exiting.")
      await self.disconnect()

  def format_output(self, msg):
    self.output += self.output_format.format(
      fr=msg['from'],
      fro=msg['from'].bare,
      nick=msg['mucnick'],
      body=msg['body'],
    )

  async def message_received(self, msg):
    logger.debug(f"Received message from {msg['from']}: {msg['body']}")
    if msg['type'] in ('chat', 'normal'):
      print(f"Message from {msg['from']}: {msg['body']}", flush=True)
      self.format_output(msg)
      self.received_messages += 1
      if self.max_messages and self.received_messages >= self.max_messages:
        await self.disconnect()

  async def groupchat_message_received(self, msg):
    logger.debug(f"Received groupchat message in {msg['from'].bare} from {msg['mucnick']}: {msg['body']}")
    if msg['mucnick'] != self.nick:  # Avoid self-response
      print(f"Room {msg['from'].bare} [{msg['mucnick']}]: {msg['body']}", flush=True)
      self.format_output(msg)
      self.received_messages += 1
      if self.max_messages and self.received_messages >= self.max_messages:
        await self.disconnect()

  async def handle_disconnection(self, event):
    logger.info("Disconnected from the XMPP server.")
    try:
      if not self.disconnect_event.is_set():
        self.disconnect_event.set()
    except Exception as e:
      logger.error(f"Error while handling disconnection: {e}")

  async def run(self):
    logger.debug("Connecting to XMPP server")
    self.connect()
    try:
      if self.timeout:
        await asyncio.wait_for(self.disconnect_event.wait(), timeout=self.timeout)
        logger.info("Timeout reached, disconnecting.")
        await self.disconnect()
      else:
        await self.disconnect_event.wait()
    except asyncio.TimeoutError:
      logger.info("Timeout, disconnecting.")
      await self.disconnect()
    except Exception as e:
      logger.error(f"Error in run loop: {e}")
      await self.disconnect()
    # logger.debug(f"SelfdevClient.run() outputs: {self.output}")
    return self.output

def main():
  """Command-line interface for the XMPP client."""
  parser = argparse.ArgumentParser(description="Command-line XMPP client for sending and receiving messages.")
  parser.add_argument("--host", type=str, help="XMPP server host (e.g., example.com).")
  parser.add_argument("--user", type=str, help="XMPP username (without domain).")
  parser.add_argument("--password", type=str, help="XMPP password.")
  parser.add_argument("--muc-host", type=str, help="MUC (multi-user chat) host.")
  parser.add_argument("--join-rooms", type=str, nargs='*', default=[], help="List of rooms to join.")
  parser.add_argument("--nick", type=str, help="Nickname in MUC rooms.")
  parser.add_argument("--recipient", type=str, help="Recipient JID (for direct messages).")
  parser.add_argument("--message", type=str, help="Message to send.")
  parser.add_argument("--max-messages", type=int, help="Exit after receiving this many messages.")
  parser.add_argument("--timeout", type=int, help="Exit after this many seconds.")
  parser.add_argument("--allow-insecure", action="store_true", help="Allow insecure SSL certificates.")
  parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging.")
  parser.add_argument("--version", action="version", version="selfdev_cli v0.0.1", help="Show version and exit.")

  args = parser.parse_args()

  host = args.host or os.getenv("XMPP_HOST")
  user = args.user or os.getenv("XMPP_USER")
  password = args.password or os.getenv("XMPP_PASSWORD")
  muc_host = args.muc_host or os.getenv("XMPP_MUC_HOST")
  nick = args.nick or os.getenv("XMPP_NICK", user)

  if not host or not user or not password:
    sys.stderr.write("Error: Host, user, and password must be specified as arguments or environment variables.\n")
    sys.exit(1)

  jid = f"{user}@{host}"

  client = SelfdevClient(jid=jid, password=password, recipient=args.recipient, message=args.message,
                         rooms=args.join_rooms, nick=nick, muc_host=muc_host,
                         max_messages=args.max_messages, timeout=args.timeout,
                         allow_insecure=args.allow_insecure or ALLOW_INSECURE, verbose=args.verbose)

  loop = asyncio.get_event_loop()
  loop.run_until_complete(client.run())

if __name__ == "__main__":
  main()
