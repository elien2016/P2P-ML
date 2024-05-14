# P2P-ML

P2P-ML allows you to build a P2P ML network with cloud integrations. It currently supports data fetching and model serving and is easily extensible to additional cloud providers and ML frameworks.

<br/>

Note:
- btpeer.py is based on https://cs.berry.edu/~nhamid/p2p/btpeer.py but has been upgraded from Python 2 to 3; the socket programming part uses some of Python 3's features. It has been modified in a few other places too, in formatting, debug messages, or functionalities.
- The GUI (btml_gui.py) uses CustomTKinter; some UI features might be platform dependent. Refer to CustomTKinter's documentation for details.

# Usage
| Message type | Message data format | Description |
| ------------ | ----------- | ----------- |
| PING | n/a | ping a peer |
| NAME | n/a | request a peer's canonical id |
| LIST | n/a | request a peer's list of peers |
| JOIN | peer-id host port | request to join a peer's list of peers |
| QUER | return-peer-id return-peer-host return-peer-port model-name ttl | query for peers capable of serving the specified model |
| RESP | model-name peer-id host port | respond to QUER |
| INFR | model-name input | request for inference using the specified model with the specified input |
| QUIT | peer-id | request to remove oneself from a peer's list of peers |
| REPL | n/a | acknowledge a message or send back results for anything that RESP doesn't handle |
| ERRO | n/a | indicate an erroneous or unsuccessful request |

# UI
dark mode:
<img width="1392" alt="Screenshot 2024-05-10 at 21 48 09" src="https://github.com/elien2016/P2P-ML/assets/65316754/e1834d2d-901f-4a49-8aef-0516d4b78289">

light mode:
<img width="1392" alt="Screenshot 2024-05-10 at 22 43 46" src="https://github.com/elien2016/P2P-ML/assets/65316754/f8b7d560-c473-43de-90ca-c939617b76f7">
