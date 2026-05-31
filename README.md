# NeKoRoPIE (Go Branch)

i serve u pie (but in GoLang)

NeKoRoPIE is a lightweight, secure, horizontally-scalable, and unopinionated Go backend that can handle REST and WebSocket endpoints. It can also perform database querying and other operations.
I provided an example MongoDB integration with basic CRUD operations tied to a Discord bot.
Every custom logic starts at `./api/rest.go` and `./api/websockets.go`. Due to its unopinionated nature, it is entirely modular and you can quite literally strip away the existing features built beyond `main.go` 🥭 and make your own.

## Practical Usage Example

"Can I use NeKoRoPIE for anything?" Absolutely. Containerize it; put it in a VPS; shard instances; add a content management system; or even real-time collaborative doc editing, like Canva or Google Docs. Anything is possible.

"How does it compare to Django?" Django is a Swiss Army Knife. NeKoRoPIE takes one blade from that Swiss Army Knife and sharpens it for absolute reliability. If for some reason you don't want to use Django and prefer to build your own framework, NeKoRoPIE will suit your needs.

"How does this Go version compare to the main Python implementation?" Go handles concurrency by default and it's generally much faster than Python. Use this branch if you're an "eugh Python so slow" typa guy.
