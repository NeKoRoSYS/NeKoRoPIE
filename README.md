# NeKoRoPIE

i serve u pie

NeKoRoPIE is a lightweight, secure, horizontally-scalable, and unopinionated Python backend that can handle REST and WebSocket endpoints. It can also perform database querying and other operations.
I provided an example MongoDB integration with basic CRUD operations tied to a Discord bot.
Every custom logic starts at `./api/rest.py` and `./api/websockets.py`. Due to its unopinionated nature, it is entirely modular and you can quite literally strip away the existing features built beyond `main.py` and make your own.

## Practical Usage Example

"Can I use NeKoRoPIE for anything?" Absolutely. Containerize it; put it in a VPS; shard instances; add a content management system; or even real-time collaborative doc editing, like Canva or Google Docs. Anything is possible.

"How does it compare to Django?" Django is a Swiss Army Knife. NeKoRoPIE takes one blade from that Swiss Army Knife and sharpens it for absolute reliability. If for some reason you don't want to use Django and prefer to build your own framework, NeKoRoPIE will suit your needs.

- Please see [NeKoRoBOT.js](https://github.com/NeKoRoSYS/NeKoRoBOT.js/) for a full-stack Discord.js bot implementation.
- Also used by our [proprietary matchmaking system](https://discord.gg/invite/cxjwAnWCjr), which also uses Discord.js.
