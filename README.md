# vine-multi-ingress

This repository contains programs to collect Vine video/user URL information
from multiple sources.

The target is a set of CouchDB databases:

- videos: `http://lothlorien.peach-bun.com:15984/vine-videos`
- users: `http://lothlorien.peach-bun.com:15984/vine-users`

The CouchDB instance allows signup, but access to these databases requires
special authorization.  Open an issue to request access.

## See also

- https://github.com/ArchiveTeam/vine-grab
- https://github.com/ArchiveTeam/vine-twitter-listener
