# Energy Monitor — PC CENTRAL / PC ECRAN (OpenComputers II 0.9.0)

Cible : Minecraft 1.21.1, NeoForge, OC2 `0.9.0+d153daa`, Mekanism / Mekanism Generators `10.7.19.85`, **MekanismOC2 1.0.0 (inchange)**.

MekanismOC2 n'est pas modifie : aucune recompilation n'est necessaire. Les programmes utilisent uniquement l'API deja exposee (`getGeneratorData` sur les peripheriques `mekanism_wind_generator` / `mekanism_generator`).

## 1. Architecture

```text
Wind Generators ──(bus interface + bus cable)──> PC CENTRAL ──(UDP 4710, JSON)──> PC ECRAN
        MekanismOC2 (devices OC2)                collecte, agregation,             affichage console
                                                 cumul persistant, API interne      (ANSI, adapte a la taille)
```

```text
central/
  main.lua          boucle principale (1 s), orchestration
  config.lua        configuration
  discovery.lua     connexion au bus OC2 et liste des peripheriques (reconnexion automatique)
  collectors.lua    lecture des generateurs, tour de lecture avec budget de temps
  collectors/wind.lua   declaration de la source "wind" (solar / nuclear : a ajouter ici plus tard)
  aggregator.lua    totaux, statistiques, fenetres 1 min / 1 h, cumul, snapshot
  persistence.lua   compteur cumule (ecriture atomique + sauvegarde de secours)
  network.lua       API interne UDP (abonnements, snapshots)
  protocol.lua      format des messages (copie identique cote ecran)
  utils.lua         unites, journal, horloge (copie identique cote ecran)
  fakebus.lua       TEST UNIQUEMENT : generateurs simules
  selftest.lua      tests automatiques
  S99z_energy_central   script de demarrage automatique
screen/
  main.lua  config.lua  network.lua  renderer.lua  protocol.lua  utils.lua  selftest.lua  run_screen.sh
```

Les donnees (compteur, journaux) sont ecrites dans `<programme>/data/` sur le disque dur de l'ordinateur. `/tmp`, `/run` et `/var/log` de l'image Linux sont en memoire : ils sont vides apres chaque redemarrage.

## 2. Materiel necessaire (par ordinateur)

Computer, RISC-V processor, flash memory « Linux firmware », hard drive « Linux root file system », 3 × memory 4M, **Network Interface Card**, et pour l'installation une **File Import/Export Card**.

Les cartes ne s'installent que ordinateur eteint. Il faut aussi :

| | PC CENTRAL | PC ECRAN |
|---|---|---|
| Bus | Bus interface contre l'ordinateur + bus cables + une bus interface contre chaque Wind Generator | aucun |
| Reseau | Network Connector sur une face qui n'est pas la face avant | idem |
| Affichage | non | Projector (alimente directement) **ou** terminal integre de l'ordinateur |
| Energie | oui (FE) | oui (FE) |

Relier les deux connecteurs avec des Network Cables (longueur maximale limitee : utiliser des connecteurs en repeteurs, 2 cables par connecteur) ou lier deux Network Tunnel Cards (une dans chaque ordinateur, liees depuis leur interface de configuration). Les deux ordinateurs doivent rester charges.

## 3. PC CENTRAL

1. Demarrer l'ordinateur, se connecter en `root`.
2. Reseau : `setup-network.lua`, puis repondre : *DHCP client* `N`, *IP address* `10.0.0.1`, *Netmask* `255.255.255.0`, *gateway* `-`, *DHCP server* `N`.
3. Controle du bus : `lsdev.lua`. Chaque Wind Generator doit apparaitre avec `mekanism_generator, mekanism_wind_generator`. Sinon, voir le depannage.
4. Import de l'installeur : `cd /root`, `import.lua`, choisir `install_central.lua`.
5. Installation : `lua install_central.lua --autostart`
   (installe dans `/root/energy_monitor/central` et copie `/etc/init.d/S99z_energy_central`).
6. Tests automatiques : `cd /root/energy_monitor/central && lua selftest.lua` → attendu : `48 reussis, 0 echecs`.
7. Premier lancement visible : `lua main.lua --verbose` (Ctrl-C pour arreter ; le compteur est sauvegarde a l'arret).
8. Demarrage automatique : redemarrer l'ordinateur, ou `/etc/init.d/S99z_energy_central start`. Suivi : `tail -f /root/energy_monitor/central/data/central.log`.

Une mise a jour relancee avec `install_central.lua` conserve votre `config.lua` (la nouvelle version est ecrite en `config.lua.new`).

## 4. PC ECRAN

1. Reseau : `setup-network.lua` avec l'IP `10.0.0.2`, memes autres reponses. Test : `ping -c 3 10.0.0.1`.
2. `cd /root`, `import.lua`, choisir `install_screen.lua`, puis `lua install_screen.lua`.
3. `cd /root/energy_monitor/screen && lua selftest.lua` → attendu : `20 reussis, 0 echecs`.
4. Verifier la mise en page et les unites sans reseau : `lua main.lua --demo` (change de grandeur toutes les 3 s, de FE a TFE).
5. Lancement reel : `lua main.lua`. Si le central n'est pas a `10.0.0.1` : `lua main.lua --central <ip>` ou modifier `config.lua`.
6. Affichage sur le projecteur, demarrage automatique (le prompt de connexion de la console projecteur est remplace ; la console du terminal integre reste disponible pour la maintenance) :

```text
cp /etc/inittab /etc/inittab.orig
sed -i 's|^tty1:2345:respawn:/sbin/getty 0 tty1.*|tty1:2345:respawn:/root/energy_monitor/screen/run_screen.sh|' /etc/inittab
grep tty1 /etc/inittab
```

Redemarrer l'ordinateur (ou `kill -HUP 1`). Pour annuler : `cp /etc/inittab.orig /etc/inittab`.

Si vous utilisez l'ecran du terminal integre, lancez simplement `lua /root/energy_monitor/screen/main.lua` apres connexion. L'interface est en ASCII, sans accents, pour rester lisible sur la police de la console.

## 5. Configuration

`central/config.lua` : `updateInterval` (1 s), `discoveryInterval` (10 s : frequence de re-scan du bus), `pollBudget` (0.6 s maximum de lecture par cycle ; les generateurs sont lus a tour de role), `staleAfter` (10 s : au-dela, un generateur non lu est compte « sans reponse » et sa production devient 0), `sources` (`wind = true`, `solar = false`, `nuclear = false`), `energy` (1 FE = 2.5 J = `feNumerator 2 / feDenominator 5`), `api` (`enabled`, `port 4710`, `subscriberTtl`, `maxSubscribers`), `storage` (dossier, `saveInterval 30`).

`screen/config.lua` : `centralAddress`, `centralPort`, `clientId`, `offlineAfter` (5 s), `renderInterval`, `maxWidth`, `useColor`, `fallbackColumns/Rows`.

Ajouter une source plus tard : creer `central/collectors/<nom>.lua` (`name`, `typeName`, `method`) et passer la source a `true` dans `config.lua`. Une source activee sans collecteur est signalee dans le journal et ignoree.

## 6. Protocole interne (version 1)

Transport : **UDP**, port `4710`, un objet JSON par datagramme (1400 octets maximum), champ `proto` obligatoire. Les champs peuvent etre ajoutes sans changer la version ; une evolution incompatible incremente `proto` (le central repond `{"type":"error","error":"unsupported_version"}`, l'ecran affiche « PROTOCOLE INCOMPATIBLE »).

| Message | Sens | Contenu |
|---|---|---|
| `subscribe` | ecran → central | `client` (nom). Renvoye toutes les 5 s (2 s tant que hors ligne). Un abonne non renouvele pendant 15 s est oublie. Le central repond tout de suite par un `snapshot`. |
| `unsubscribe` | ecran → central | desabonnement propre |
| `get_snapshot` | client → central | reponse immediate d'un `snapshot` (utile pour un futur client ponctuel) |
| `snapshot` | central → abonnes | envoye chaque seconde, voir ci-dessous |
| `error` | central → client | `error`, `supported` |

Tous les acces sont en lecture seule : aucun message ne modifie le central.

Contenu d'un `snapshot` (toutes les valeurs d'energie en entiers FE, sauf `production.j_t` en Joules) :

```text
proto, type="snapshot", seq, ts, uptime, status="online", bus="ok"|"down"
production   { j_t, fe_t, fe_s }
generators   { total, active, invalid, stale }
storage      { fe, capacity_fe }          somme des tampons internes des generateurs
cumulative   { fe }
stats        { produced_1m_fe, avg_1m_fe_s, produced_1h_fe, avg_1h_fe_s, peak_fe_s }
sources      { wind{enabled,total,active,fe_t,fe_s}, solar{enabled}, nuclear{enabled} }
poll         { polled, failed, ms }
```

Calcul : `fe_t = J/t ÷ 2.5`, `fe_s = fe_t × 20`. Les totaux sont calcules sur la somme exacte en Joules puis convertis une seule fois (la somme des `fe_t` individuels arrondis peut differer de quelques FE).

## 7. Procedure de test

Les chiffres « attendus » du tableau supposent des generateurs reels identiques de 400 J/t (160 FE/t) ; avec un vent different, comparez avec `getGeneratorData` d'un generateur (`lua`, puis `d = require("devices")`, `g = d:find("mekanism_wind_generator")`, `t = g:getGeneratorData()`).

| # | Action | Resultat attendu |
|---|---|---|
| 1 | 1 Wind Generator, central et ecran lances | l'ecran affiche `1 / 1 actifs` et la production du generateur (`production_fe_t` / `production_fe_s` de `getGeneratorData`) |
| 2 | Ajouter des Wind Generators (avec leur bus interface) ; attendre ≤ 10 s | `total` augmente ; la production egale la somme des `production_j_t` ÷ 2.5 (a quelques FE d'arrondi pres) |
| 3 | Desactiver un generateur (redstone / arret) | `actifs` diminue de 1 ; sa production devient 0 FE/t |
| 4 | Eteindre le PC ECRAN | le central continue ; le journal indique `PC ecran deconnecte` apres ≤ 15 s |
| 5 | Rallumer le PC ECRAN | l'ecran se reconnecte seul et affiche les donnees (≤ 5 s) |
| 6 | Redemarrer le PC CENTRAL | `production.dat` restaure : le cumul affiche reprend la valeur d'avant (+ rien pendant l'arret) |
| 7 | Redemarrer Minecraft / le serveur | meme controle ; si l'ordinateur ne se rallume pas seul, l'allumer (voir limites) |
| 8 | Casser un generateur en marche | aucun plantage ; `total` baisse apres le prochain scan (≤ 10 s) ; production correspondante retiree |
| 9 | Couper le reseau (retirer un cable), attendre > 5 s | l'ecran affiche `OFFLINE` et `CENTRAL OFFLINE` ; en remettant le cable, retour automatique a `ONLINE` |
| 10 | Solar / Nuclear = `false` | l'ecran affiche `Solaire : desactive`, `Nucleaire : desactive`, tout continue |
| 11 | Grandes valeurs : `lua main.lua --demo` sur l'ecran | `950 FE/s`, `15.40 kFE/s`, `2.80 MFE/s`, `4.20 GFE/s`, `1.10 TFE/s` |

Tests de charge sans construire de generateurs (arreter d'abord le vrai central : `/etc/init.d/S99z_energy_central stop`) :

```text
lua main.lua --verbose --fake-bus 1   --fake-j 400
lua main.lua --verbose --fake-bus 10  --fake-j 400        (attendu : 1600 FE/t, 32.00 kFE/s)
lua main.lua --verbose --fake-bus 500 --fake-j 400 --fake-latency 0.002
lua main.lua --verbose --fake-bus 1   --fake-j 125000000000   (attendu : 1.00 TFE/s)
```

Le mode simule utilise son propre fichier de compteur (`production_fake.dat`) : le vrai compteur n'est pas touche. Rien n'est simule cote reseau.

## 8. Limites connues

- **Non teste dans Minecraft** : la logique, le protocole UDP, l'affichage et la persistance ont ete testes avec un Lua 5.4 hors du jeu et un bus simule ; l'appel reel `bus:invoke(..., "getGeneratorData")` suit exactement le code du `devices` d'OC2 0.9.0, mais seule la procedure ci-dessus le confirmera en jeu.
- Bus OC2 : environ 128 elements maximum par ordinateur (cables + interfaces) → quelques dizaines de generateurs par PC. Au-dela, il faut un second ordinateur collecteur (pas encore implemente ; le format `sources` du snapshot est prevu pour l'accueillir).
- Un appel de bus qui ne recoit aucune reponse peut bloquer jusqu'a 35 s (delai du client OC2) ; le central reprend ensuite normalement.
- Le cumul suppose que l'horloge de la VM suit le temps de jeu ; un serveur qui lag peut fausser legerement le cumul. Chaque intervalle est plafonne a 3 s.
- « Energie stockee » = somme des tampons internes des Wind Generators, pas des Energy Cubes ni d'une Induction Matrix.
- L'ordinateur central doit etre sous tension, charge, et sa partition de disque non corrompue ; apres un arret brutal du serveur, le compteur peut perdre au plus 30 s.
- L'API interne n'est pas chiffree ni authentifiee (reseau Minecraft prive, lecture seule). L'API WEB future n'est pas implementee.

## 9. Depannage

| Symptome | Piste |
|---|---|
| `lsdev.lua` ne montre aucun Wind Generator | bus interface collee au generateur ? bus cable jusqu'a une bus interface collee a l'ordinateur ? onglet de l'ordinateur : « Bus Incomplete » / « Bus Too Complex » / « Multiple Bus Controllers » |
| Journal : `bus OC2 inaccessible` | ordinateur recemment demarre ; le central reessaie toutes les 5 s |
| Ecran bloque sur `CONNEXION...` | `ping 10.0.0.1` ; `ifconfig` ; carte reseau installee ordinateur eteint ; adresse dans `screen/config.lua` |
| Journal : `Erreur reseau: API non demarree` | port 4710 deja utilise (un autre central tourne ?) |
| `generateurs sans reponse` sur l'ecran | generateur retire, chunk decharge ou bus trop lent ; voir `data/central.log` |
| Remettre le cumul a zero | arreter le central, supprimer `data/production.dat`, `.bak` et `.tmp`, redemarrer |
