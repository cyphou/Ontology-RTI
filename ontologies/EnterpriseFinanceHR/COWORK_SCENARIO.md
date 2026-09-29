# Scenario Cowork : Insights + Forecast + PPT + Reunion Teams (execution manuelle)

Ce document decrit, etape par etape, comment executer vous-meme le pipeline
"M365 Cowork" pour Enterprise Finance + HR : poser des questions metier sur le
modele semantique, calculer un forecast, generer un deck PowerPoint, et
planifier une reunion Teams de restitution.

Tous les scripts se trouvent dans `ontologies/EnterpriseFinanceHR/` et
`ontologies/EnterpriseFinanceHR/tools/`.

## Prerequis

- PowerShell 7+ (`pwsh`) avec le module Az deja connecte a la souscription
  `pde-demo-hr` (`Connect-AzAccount -Tenant <tenant-id>`).
- Node.js 18+ (verifie : `node -v`).
- Une reunion Teams necessite le module `Microsoft.Graph.Calendar`
  (installe automatiquement par le script si absent).

## Etape 1 — Authentification Power BI (si le token a expire)

```powershell
Connect-AzAccount -Tenant "<tenant-id>"
```

Les scripts utilisent `Get-AzAccessToken -ResourceUrl 'https://analysis.windows.net/powerbi/api'`
en interne ; pas besoin de scope Graph a ce stade.

## Etape 2 — Poser les questions + calculer le forecast

```powershell
cd 'C:\GitHub Project\OntologyAccelerator'
.\ontologies\EnterpriseFinanceHR\Generate-CoworkInsights.ps1
```

Ce script :
- Execute 6 questions metier en DAX contre le modele Direct Lake
  `EnterpriseFinanceHRModel` (budget/variance, effectifs, recrutement,
  remuneration, presence, tendance de depense).
- Calcule un forecast lineaire (regression) sur 3 periodes a partir de
  l'historique Actual/Headcount (24 periodes fiscales).
- Ecrit `artifacts/cowork-insights.json` (le fichier actuellement ouvert dans
  votre editeur).

Parametres optionnels : `-ForecastPeriods 6`, `-OutFile <chemin>`.

**Point d'attention** : la mesure `factactualledger[Vacancy Rate]` est cassee
dans le modele (erreur `SUM` sur une colonne texte) — le script ne l'appelle
pas volontairement. Ne pas l'ajouter sans corriger le modele au prealable.

## Etape 3 — Generer le deck PowerPoint

```powershell
cd 'ontologies\EnterpriseFinanceHR\tools'
npm install          # une seule fois, installe pptxgenjs
node generate-insights-deck.mjs
```

Sortie : `artifacts/EnterpriseFinanceHR-Insights.pptx` (4 slides : titre,
Q&R, forecast en graphique barres, prochaines etapes).

Pour regenerer a partir d'un autre fichier d'insights ou vers un autre
chemin :

```powershell
node generate-insights-deck.mjs '<chemin-insights.json>' '<chemin-sortie.pptx>'
```

## Etape 4 — Controle qualite du deck (recommande avant diffusion)

```powershell
python -m markitdown '..\..\..\artifacts\EnterpriseFinanceHR-Insights.pptx'
```

Verifier : pas de texte tronque/mal encode (tirets, caracteres speciaux),
pas de `?` a la place d'une icone, montants bien formates avec separateurs
de milliers. En cas de souci, editer `generate-insights-deck.mjs` en
n'utilisant que des caracteres ASCII simples dans les chaines (eviter
tiret cadratin `—`, puce `•`, emoji — source de mojibake constatee).

Il n'y a pas de LibreOffice installe sur ce poste : impossible de generer
des captures visuelles des slides localement. Ouvrir le fichier dans
PowerPoint pour une verification visuelle finale.

## Etape 5 — Planifier la reunion Teams

**Une reunion placeholder existe deja** (creee lors de la session
precedente, sans participants) :
- Sujet : *Enterprise Finance + HR - AI Insights Review*
- 10 septembre 2026, 10h00–10h30 (Europe/Paris)
- Lien Teams : `https://teams.microsoft.com/meet/232348013733842?p=UlhiO9zMBJpiJJYIJq`

Si vous voulez la reutiliser, ouvrez-la simplement dans Outlook/Teams et
ajoutez les participants + piece jointe. Sinon, pour en creer une nouvelle
(ou avec des participants des la creation) :

```powershell
cd 'ontologies\EnterpriseFinanceHR\tools'
.\Create-TeamsMeeting.ps1 -DaysFromNow 2 -StartTime '10:00' `
    -AttendeeEmails @('alice@contoso.com','bob@contoso.com')
```

Le script :
- Installe `Microsoft.Graph.Calendar` si besoin (une seule fois).
- Demande une connexion interactive `Connect-MgGraph -Scopes 'Calendars.ReadWrite'`
  (fenetre de consentement a la premiere execution).
- Cree l'evenement avec reunion Teams integree (`isOnlineMeeting = $true`)
  et le corps HTML des 6 questions/reponses.
- Affiche le lien Teams et le lien Outlook du nouvel evenement.

Pour previsualiser le JSON envoye sans rien creer :

```powershell
.\Build-TeamsMeetingRequest.ps1 | Out-File apercu-reunion.json
```

## Etape 6 (optionnel) — Joindre le PPT a l'invitation

`Create-TeamsMeeting.ps1` ne joint pas le fichier automatiquement (l'upload
de piece jointe binaire via Graph demande un appel separe). Le plus simple :
ouvrir l'evenement cree dans Outlook, cliquer *Joindre un fichier*, et
selectionner `artifacts/EnterpriseFinanceHR-Insights.pptx`.

## Recapitulatif des fichiers produits

| Fichier | Role |
|---|---|
| `ontologies/EnterpriseFinanceHR/Generate-CoworkInsights.ps1` | Q&R + forecast -> JSON |
| `artifacts/cowork-insights.json` | Donnees intermediaires (questions, reponses, serie historique, forecast) |
| `ontologies/EnterpriseFinanceHR/tools/generate-insights-deck.mjs` | JSON -> PPTX |
| `artifacts/EnterpriseFinanceHR-Insights.pptx` | Deck final |
| `ontologies/EnterpriseFinanceHR/tools/Build-TeamsMeetingRequest.ps1` | Previsualise le corps JSON de la reunion |
| `ontologies/EnterpriseFinanceHR/tools/Create-TeamsMeeting.ps1` | Cree reellement la reunion Teams via Graph |

## Pieges connus (deja rencontres et corriges dans les scripts)

- PowerShell : `` `$(...) `` dans une chaine entre guillemets doubles est un
  texte litteral, pas une expression evaluee — toujours utiliser `-f` pour
  interpoler un montant avec le prefixe `$`.
- pptxgenjs : eviter tiret cadratin, puce Unicode et emoji dans les
  `addText` — remplaces par `-`, `|` et des cercles colores avec une lettre.
- Le modele contient une mesure cassee (`Vacancy Rate`) a ne pas interroger.
