                    ┌────────────────────┐
                    │ Binance WebSocket  │
                    │ BTCUSDT / ETHUSDT  │
                    └─────────┬──────────┘
                              │
                              ▼
                    ┌────────────────────┐
                    │      EXTRACT       │
                    │ Recevoir le flux   │
                    └─────────┬──────────┘
                              │
                              ▼
                    ┌────────────────────┐
                    │     TRANSFORM      │
                    │ Nettoyer           │
                    │ Filtrer            │
                    │ Agréger / minute   │
                    │ Détecter events    │
                    └───────┬────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
   ┌────────────────────┐       ┌────────────────────┐
   │       MySQL        │       │      MongoDB       │
   │ Données propres    │       │ Events / anomalies │
   │ Agrégats minute    │       │ Logs flexibles     │
   └─────────┬──────────┘       └─────────┬──────────┘
             │                            │
             └─────────────┬──────────────┘
                           ▼
                 ┌────────────────────┐
                 │      Power BI      │
                 │ Dashboard final    │
                 └────────────────────┘



## Bloc 1 — Source live


Rôle
Recevoir les données live depuis Binance.
Ce que tu dois comprendre
Binance envoie beaucoup de messages.
Donc ton système ne doit pas dire :

Je stocke tout.

Il doit dire :

Je reçois tout, mais je garde seulement ce qui est utile.


## Bloc 2 — Transformation


C’est le cœur du projet.
Tu vas transformer chaque trade brut en information utile.



BTCUSDT
price = 65000                              
quantity = 0.002
timestamp = 10:31:15

        |
        |
        |
        |

 BTCUSDT
minute = 10:31
prix_moyen = 65010
prix_min = 64980
prix_max = 65050
volume_total = 1.45
nombre_trades = 320




## Bloc 3 — MySQL

MySQL va stocker les données structurées.
Tu dois penser MySQL comme :

La base propre pour analyser.

Donc MySQL ne doit pas recevoir chaque message brut.


MySQL doit recevoir :

- instruments
- agrégats par minute
- état du pipeline
- qualité des données


## Bloc 4 — MongoDB

MongoDB va stocker les données événementielles.

Exemple :

- variation forte du prix
- volume anormal
- erreur de donnée
- message exceptionnel
- log important

MongoDB est utile parce que chaque événement peut avoir une structure différente.