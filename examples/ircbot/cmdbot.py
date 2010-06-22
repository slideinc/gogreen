import basebot


class CmdBot(basebot.Bot):
    class commander(object):
        def __init__(self, bot):
            self.bot = bot

        def ping(self, rest):
            return 'pong'

    def __init__(self, *args, **kwargs):
        super(CmdBot, self).__init__(*args, **kwargs)
        self.cmds = self.commander(self)

    def on_privmsg(self, cmd, args, prefix):
        parent = super(CmdBot, self)
        if hasattr(parent, "on_privmsg"):
            parent.on_privmsg(cmd, args, prefix)

        sender = prefix.split("!", 1)[0]
        to, msg = args

        if to in self.in_rooms:
            if not msg.startswith(self.nick):
                return
            rest = msg[len(self.nick):].lstrip(": ")
        else:
            rest = msg

        if " " in rest:
            cmd_name, rest = rest.split(" ", 1)
        else:
            cmd_name, rest = rest, ""

        handler = getattr(self.cmds, cmd_name, None)
        result = None
        if handler:
            result = handler(rest)

        if result:
            if to in self.in_rooms:
                self.message(to, "%s: %s" % (sender, result))
            elif to == self.nick:
                self.message(sender, result)
            else:
                raise basebot.IRCError("unknown target: %s" % to)
