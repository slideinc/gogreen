import socket

from gogreen import coro


class Bot(coro.Thread):
    def __init__(self,
            server_address,
            nick,
            username=None,
            realname=None,
            password=None,
            rooms=None):
        super(Bot, self).__init__()
        self.server_address = server_address
        self.nick = nick
        self.username = username or nick
        self.realname = realname or nick
        self.password = password
        self.rooms = rooms or []

    def cmd(self, cmd, *args):
        args = map(str, args)
        if args and " " in args[-1] and not args[-1].startswith(":"):
            args[-1] = ":" + args[-1]

        # lock this so separate outgoing commands can't overlap
        self.write_lock.lock()
        try:
            self.sock.sendall(" ".join([cmd.upper()] + args) + "\n")
        finally:
            self.write_lock.unlock()

    def connect(self):
        raw_sock = socket.socket()
        self.sock = coro.coroutine_socket(raw_sock)
        self.sock.connect(self.server_address)
        self.write_lock = coro.coroutine_lock()

    def register(self):
        self.cmd("nick", self.nick)
        self.cmd("user", self.username, 0, 0, self.realname)
        if self.password:
            self.cmd("pass", self.password)
        self.post_register()

    def change_nick(self, nick):
        self.nick = nick
        self.cmd("nick", nick)

    def join_rooms(self):
        for room in self.rooms:
            if hasattr(room, "__iter__"):
                # room is (room, password)
                self.cmd("join", *room)
            else:
                self.cmd("join", room)

    def message(self, target, message):
        self.cmd("privmsg", target, message)

    def quit(self, message=None):
        self.child_wait()
        if message is None:
            self.cmd("quit")
        else:
            self.cmd("quit", message)

    def run(self):
        self.running = True

        self.connect()
        self.register()
        self.join_rooms()

        sockfile = self.sock.makefile()

        while self.running:
            line = sockfile.readline()

            if not line:
                break

            worker = BotWorker(self, args=(line,))
            worker.start()

    def handle_ping(self, args, prefix):
        self.cmd("pong", args[0])

    def handle_nick(self, args, prefix):
        self.on_nick_changed(args[0])

    def on_nick_changed(self, new_nick):
        pass

    def default_handler(self, cmd, args, prefix):
        pass

    def default_on_reply(self, code, args, prefix):
        pass

    def post_register(self):
        pass


class BotWorker(coro.Thread):
    def __init__(self, bot, *args, **kwargs):
        super(BotWorker, self).__init__(*args, **kwargs)
        self.bot = bot

    def _parse_line(self, line):
        prefix = None
        if line.startswith(":"):
            prefix, line = line.split(" ", 1)
            prefix = prefix[1:]

        cmd, line = line.split(" ", 1)

        if line.startswith(":"):
            args = [line[1:]]
        elif " :" in line:
            args, final_arg = line.split(" :", 1)
            args = args.split(" ")
            args.append(final_arg)
        else:
            args = line.split(" ")

        return cmd, args, prefix

    def run(self, line):
        cmd, args, prefix = self._parse_line(line.rstrip('\r\n'))
        if cmd.isdigit():
            cmd = int(cmd)
            if _code_is_error(cmd):
                raise IRCServerError(cmd, REPLY_CODES[cmd], *args)
            handler = getattr(
                    self.bot, "on_reply_%d" % cmd, self.bot.default_on_reply)
        else:
            handler = getattr(
                    self.bot, "on_%s" % cmd.lower(), self.bot.default_handler)

        handler(cmd, args, prefix)


class IRCError(Exception):
    pass


class IRCServerError(IRCError):
    pass


def _code_is_error(code):
    return code >= 400

# http://irchelp.org/irchelp/rfc/chapter6.html
REPLY_CODES = {
    200: "RPL_TRACELINK",
    201: "RPL_TRACECONNECTING",
    202: "RPL_TRACEHANDSHAKE",
    203: "RPL_TRACEUNKNOWN",
    204: "RPL_TRACEOPERATOR",
    205: "RPL_TRACEUSER",
    206: "RPL_TRACESERVER",
    208: "RPL_TRACENEWTYPE",
    211: "RPL_STATSLINKINFO",
    212: "RPL_STATSCOMMANDS",
    213: "RPL_STATSCLINE",
    214: "RPL_STATSNLINE",
    215: "RPL_STATSILINE",
    216: "RPL_STATSKLINE",
    218: "RPL_STATSYLINE",
    219: "RPL_ENDOFSTATS",
    221: "RPL_UMODEIS",
    241: "RPL_STATSLLINE",
    242: "RPL_STATSUPTIME",
    243: "RPL_STATSOLINE",
    244: "RPL_STATSHLINE",
    251: "RPL_LUSERCLIENT",
    252: "RPL_LUSEROP",
    253: "RPL_LUSERUNKNOWN",
    254: "RPL_LUSERCHANNELS",
    255: "RPL_LUSERME",
    256: "RPL_ADMINME",
    257: "RPL_ADMINLOC1",
    258: "RPL_ADMINLOC2",
    259: "RPL_ADMINEMAIL",
    261: "RPL_TRACELOG",
    300: "RPL_NONE",
    301: "RPL_AWAY",
    302: "RPL_USERHOST",
    303: "RPL_ISON",
    305: "RPL_UNAWAY",
    306: "RPL_NOAWAY",
    311: "RPL_WHOISUSER",
    312: "RPL_WHOISSERVER",
    313: "RPL_WHOISOPERATOR",
    314: "RPL_WHOWASUSER",
    315: "RPL_ENDOFWHO",
    317: "RPL_WHOISIDLE",
    318: "RPL_ENDOFWHOIS",
    319: "RPL_WHOISCHANNELS",
    321: "RPL_LISTSTART",
    322: "RPL_LIST",
    323: "RPL_LISTEND",
    324: "RPL_CHANNELMODEIS",
    331: "RPL_NOTOPIC",
    332: "RPL_TOPIC",
    341: "RPL_INVITING",
    342: "RPL_SUMMONING",
    351: "RPL_VERSION",
    352: "RPL_WHOREPLY",
    353: "RPL_NAMREPLY",
    364: "RPL_LINKS",
    365: "RPL_ENDOFLINKS",
    366: "RPL_ENDOFNAMES",
    367: "RPL_BANLIST",
    368: "RPL_ENDOFBANLIST",
    369: "RPL_ENDOFWHOWAS",
    371: "RPL_INFO",
    372: "RPL_MOTD",
    374: "RPL_ENDOFINFO",
    375: "RPL_MOTDSTART",
    376: "RPL_ENDOFMOTD",
    381: "RPL_YOUREOPER",
    382: "RPL_REHASHING",
    391: "RPL_TIME",
    392: "RPL_USERSSTART",
    393: "RPL_USERS",
    394: "RPL_ENDOFUSERS",
    395: "RPL_NOUSERS",

    401: "ERR_NOSUCHNICK",
    402: "ERR_NOSUCHSERVER",
    403: "ERR_NOSUCHCHANNEL",
    404: "ERR_CANNOTSENDTOCHAN",
    405: "ERR_TOOMANYCHANNELS",
    406: "ERR_WASNOSUCHNICK",
    407: "ERR_TOOMANYTARGETS",
    409: "ERR_NOORIGIN",
    411: "ERR_NORECIPIENT",
    412: "ERR_NOTEXTTOSEND",
    413: "ERR_NOTOPLEVEL",
    414: "ERR_WILDTOPLEVEL",
    421: "ERR_UNKNOWNCOMMAND",
    422: "ERR_NOMOTD",
    423: "ERR_NOADMININFO",
    424: "ERR_FILEERROR",
    431: "ERR_NONICKNAMEGIVEN",
    432: "ERR_ERRONEUSENICKNAME",
    433: "ERR_NICKNAMEINUSE",
    436: "ERR_NICKCOLLISION",
    441: "ERR_USERNOTINCHANNEL",
    442: "ERR_NOTONCHANNEL",
    443: "ERR_USERONCHANNEL",
    444: "ERR_NOLOGIN",
    445: "ERR_SUMMONDISABLED",
    446: "ERR_USERSDISABLED",
    451: "ERR_NOTREGISTERED",
    461: "ERR_NEEDMOREPARAMS",
    462: "ERR_ALREADYREGISTERED",
    463: "ERR_NOPERFORMHOST",
    464: "ERR_PASSWDMISMATCH",
    465: "ERR_YOUREBANNEDCREEP",
    467: "ERR_KEYSET",
    471: "ERR_CHANNELISFULL",
    472: "ERR_UNKNOWNMODE",
    473: "ERR_INVITEONLYCHAN",
    474: "ERR_BANNEDFROMCHAN",
    475: "ERR_BADCHANNELKEY",
    481: "ERR_NOPRIVILEGES",
    482: "ERR_CHANOPRIVSNEEDED",
    483: "ERR_CANTKILLSERVER",
    491: "ERR_NOOPERHOST",
    501: "ERR_UMODEUNKNOWNFLAG",
    502: "ERR_USERSDONTMATCH",
}
