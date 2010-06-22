import basebot


class OpsHolderBot(basebot.Bot):
    '''A most inactive bot, it will just sit in a room and hold any chanops you
    give it, doling out chanops upon request (when you provide the password)

    to get it to op you, /msg it with the password, a space, and the room name

    to start it up, do something like this:
    >>> bot = OpsHolderBot(
    >>>         ("example.irc.server.com", 6667),
    >>>         "bot_nick",
    >>>         password=irc_password_for_nick,
    >>>         ops_password=password_for_giving_chanops,
    >>>         rooms=["#list", "#of", "#channels", "#to", "#serve"])
    >>> bot.start()
    >>> coro.event_loop()
    '''
    def __init__(self, *args, **kwargs):
        self._ops_password = kwargs.pop("ops_password")
        super(OpsHolderBot, self).__init__(*args, **kwargs)
        self._chanops = {}

    def join(self, room, passwd=None):
        super(OpsHolderBot, self).join(room, passwd)
        self._chanops[room] = False

    def on_mode(self, cmd, args, prefix):
        # MODE is how another user would give us ops -- test for that case
        context, mode_change = args[:2]
        if context.startswith("#") and \
                mode_change.startswith("+") and \
                "o" in mode_change and\
                len(args) > 2 and args[2] == self.nick:
            self._chanops[context] = True

    def on_privmsg(self, cmd, args, prefix):
        # ignore channels
        if args[0] != self.nick:
            return

        sender = prefix.split("!", 1)[0]
        passwd, roomname = args[1].rsplit(" ", 1)

        if passwd == self._ops_password:
            if self._chanops.get(roomname):
                # grant chanops
                self.cmd("mode", roomname, "+o", sender)
            else:
                self.message(sender, "I don't have ops to give in that room")
        else:
            self.message(sender, "sorry, wrong password")

    def on_reply_353(self, code, args, prefix):
        # 353 is sent when joining a room -- this tests to see if we are given
        # chanops upon entrance (we are the first here, creating the room)
        names = args[-1]
        if "@" + self.nick in names.split(" "):
            self._chanops[args[-2]] = True
