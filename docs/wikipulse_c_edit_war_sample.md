# WikiPulse edit-war sample labels (7-day archive)

Labelled 2026-10-24 for C by an AI assistant (one rater, edit summaries only; not yet reviewed by a second person). Input: `scripts/evaluate_wikipulse_edit_wars.py` on `hdfs://namenode:8020/wikipulse/raw_backfill` Delta version 348 (2026-10-17 12:00 to 10-24 18:15 UTC), which found 1,052 candidate windows on 209 pages. Fifty pages were drawn at random (Python `random.Random(20261024).shuffle`) and labelled with the rubric in [B's evaluation](wikipulse_b_edit_war_evaluation.md), from the ordered revert summaries of each page's strongest window. Uncertain means the summaries do not show which kind of exchange it is.

Result: 8 positive, 33 negative, 9 uncertain. Precision 8/41 = 19.5% (Wilson 95% CI 10.2–34.0%) excluding uncertain pages; 8/50 = 16.0% counting them as negative.

| Wiki | Page | Reverts / users | Label | Category | Evidence |
| --- | --- | --- | --- | --- | --- |
| dewiki | Danz op de Deel | 7 / 2 | uncertain | unclear | Repeated rollback vs. undo between a registered editor and a temporary account; summaries only trade accusations ("unbegründeter Revert", "Retourkutsche", "siehe VM", "Geh und such dir ein Hobby!") with no content argument. |
| dewiki | Elliptische Modulform | 3 / 2 | negative | patrol_tool | Two patrollers roll back the same temporary account, one via Huggle (HG); routine anti-vandal reversion. |
| dewiki | Peer Heinlein | 3 / 2 | positive | content_dispute | Mary Joanna cites WP:BEL and the firm's 2004 founding date; Gelli63 counters "genau seit 2004 so nun konkreter": a sourcing/factual disagreement. |
| enwiki | 2025 JD Vance speech at the Munich Security Conference | 3 / 2 | negative | patrol_tool | Interceptor reverts "commentary in article" by a temporary account, which only replies with a bare Undo. |
| enwiki | Amit Guluzade | 3 / 2 | negative | patrol_tool | Two patrollers remove unsourced content ("Interceptor", "Instagram is not a reliable source"); the temporary account never argues back. |
| enwiki | BBC controversies | 4 / 2 | negative | patrol_tool | Interceptor reverts "non-constructive edits" by a temporary account that answers with bare Undos. |
| enwiki | Butters Stotch | 6 / 4 | negative | vandalism_rollback | Several patrollers and ClueBot NG revert Samme3124, explicitly labelled "Vandalism" (UV tool). |
| enwiki | Carl Bildt | 4 / 3 | uncertain | unclear | Two registered editors roll back Asdf1223344, who answers with bare Undos; no summary states a reason on either side. |
| enwiki | Central Pointe station | 5 / 2 | negative | patrol_tool | Interceptor and WikiShield revert "non-constructive edits" by one temporary account. |
| enwiki | County (United States) | 6 / 2 | negative | patrol_tool | Interceptor repeatedly reverts "non-constructive edits" by a temporary account that only issues bare Undos. |
| enwiki | Cube (film series) | 7 / 3 | negative | sock_cleanup | Reverts are labelled "Block evasion by User:Didaka90" / "rvv block evasion"; the temp account replies with personal attacks. |
| enwiki | Dallas | 3 / 2 | negative | vandalism_rollback | MrOllie and ClueBot NG ("possible vandalism") revert the same temporary account. |
| enwiki | Dan Povenmire | 3 / 2 | negative | sock_cleanup | Magical Golden Whip reverts under WP:BANREVERT, i.e. edits by a banned user. |
| enwiki | Detroit Rock City | 3 / 2 | positive | content_dispute | Temp account argues Oricon News is a reliable source for the film adaptation; Rift reverts citing MOS:POPCULT: a sourcing/relevance dispute. |
| enwiki | EMD SD70 series | 3 / 2 | uncertain | unclear | One side complains about an image choice but with threats ("Remove This TRAIN ... or Banned"); the other only cites WP:CIVIL, so no two-sided content position is visible. |
| enwiki | Four (One Direction album) | 9 / 6 | negative | vandalism_rollback | Interceptor, rollbacks and ClueBot NG ("possible vandalism") revert a string of temporary accounts posting bare "rv"/"rvv". |
| enwiki | Ghana | 4 / 2 | uncertain | unclear | Annh07 and Morkoz revert several different accounts with default rollback/undo summaries that give no reason. |
| enwiki | Go back to where you came from | 5 / 2 | negative | patrol_tool | Materialscientist reverts via Interceptor; the temporary account merely mimics the summary ("Reverting non constructive edits by Materialscientist"). |
| enwiki | Guy Debord | 3 / 2 | negative | vandalism_rollback | ClueBot NG ("possible vandalism") and Interceptor revert temporary accounts; the second user is a bot. |
| enwiki | Hannes Hólmsteinn Gissurarson | 4 / 3 | negative | vandalism_rollback | ClueBot NG flags the temp account's BLP edit as vandalism and a patroller restores it ("NOT openly gay"). |
| enwiki | Horrid Henry (TV series) | 8 / 6 | negative | sock_cleanup | Edits by Nicholas Cruise1/2/3 are rolled back in sequence, and the first summary is "reverting a sock". |
| enwiki | Indeterminate form | 3 / 2 | positive | content_dispute | Mathematical disagreement over whether 1^infinity is indeterminate, with Jacobolus citing a JSTOR source and the temp account disputing what it says. |
| enwiki | Japanese art | 4 / 3 | negative | vandalism_rollback | Patrollers revert several unrelated vandal accounts (e.g. "Wikisgay123456789") via Interceptor. |
| enwiki | Justin Chart | 3 / 2 | uncertain | unclear | Rift restores a citation for the subject's age ("Rv removal of citation for age"), but the other side gives no reason, so it could be unexplained removal or a BLP concern. |
| enwiki | L. J. Collier | 3 / 2 | negative | patrol_tool | ESkog reverts a temporary account via the AntiVandal (AV) tool; the temp only issues a bare Undo. |
| enwiki | List of ABS-CBN Studios original drama series | 3 / 2 | negative | patrol_tool | Huggle ("unexplained content removal") and ClueBot NG revert the same temporary account. |
| enwiki | List of films released in IMAX (2020s) | 4 / 2 | positive | content_dispute | Extended argument over which films qualify as "Filmed for IMAX" (Lake Changjin, Dead Reckoning, aspect ratio). |
| enwiki | Lulism | 3 / 2 | positive | content_dispute | Two registered editors trade undos, one saying "per previous" and the other "talk to us first before changing it": a BRD content disagreement with no vandalism signal. |
| enwiki | Michael Gove | 3 / 2 | negative | patrol_tool | Interceptor removes unsourced content; the temporary account only copies a "Reverting non-constructive edits" summary back. |
| enwiki | Mike Hallett (American football) | 3 / 2 | negative | patrol_tool | PhilKnight rolls back "good faith ... unsourced changes" and the temp account replies with a bare Undo. |
| enwiki | Nancy Ajram | 5 / 4 | negative | vandalism_rollback | ClueBot NG ("possible vandalism"), Interceptor and patrollers revert several temporary accounts. |
| enwiki | National Rifle Association | 8 / 5 | negative | vandalism_rollback | A temporary account and Sbsbbab undo years-old revisions and are reverted as "Vandalism" (RW) / "Reverting vandalism or test edit". |
| enwiki | Nuix | 4 / 3 | uncertain | unclear | A random-string account undoes a revision claiming "Rv vandalism" and is rolled back by two editors without stated reasons. |
| enwiki | Nîmes | 3 / 2 | negative | vandalism_rollback | T430s reverts the temporary account with an explicit "Vandalism" (RW 16.1) label. |
| enwiki | Oakgrove Integrated College | 3 / 2 | uncertain | unclear | Bare "Revert" / "Undid Vandalism" / Undo exchange on a school article; the claim of vandalism cannot be checked from the summaries. |
| enwiki | Overwatch | 9 / 4 | negative | vandalism_rollback | ClueBot NG, UV ("Non-constructive edit") and Interceptor revert a succession of vandal accounts. |
| enwiki | Ozark hellbender | 6 / 2 | negative | vandalism_rollback | Two patrollers revert Thisguyisawesomedudee with "vandalism" (WikiShield) and "Rvv". |
| enwiki | Python (programming language) | 4 / 3 | positive | content_dispute | A temp account adds Steering Council information, promises to add a source, and is reverted as "Incorrect ... contradicts PEP 8100": a factual/sourcing disagreement. |
| enwiki | Right-wing populism | 3 / 2 | positive | content_dispute | Disagreement over listing Erdogan: Belbury cites WP:OR and the temp account adds a reference and argues he qualifies more than Le Pen. |
| enwiki | Scots language | 9 / 5 | negative | vandalism_rollback | Huggle, Interceptor and rollbacks revert several temporary accounts and "Scotsisnotareallamg". |
| enwiki | Simon Fraser (explorer) | 6 / 4 | negative | patrol_tool | Interceptor and three patrollers revert "unsourced content"; the temp account's only rationale is "proper information from goolge". |
| enwiki | Terrytoons | 3 / 2 | negative | sock_cleanup | Magical Golden Whip reverts under "Ban revert" / WP:BANREVERT. |
| enwiki | The Cat in the Hat Comes Back | 9 / 7 | negative | sock_cleanup | Temporary accounts post "revert lta" / "rv lta" and are reverted by UV and ClueBot NG; this is LTA activity on both sides. |
| enwiki | Tweenies | 5 / 2 | uncertain | unclear | Beyhive Navy rolls back one temporary account three times with default summaries, and the temp replies with bare Undos; no reason is given. |
| enwiki | WD J1330+6435 | 3 / 2 | negative | patrol_tool | Interceptor reverts "non-constructive edits" by three different temporary accounts. |
| enwiki | Walt Disney World | 4 / 3 | negative | vandalism_rollback | Interceptor and VandalHandle ("vandalism") revert temporary accounts, ending with "No it's not." |
| enwiki | West Virginia | 15 / 8 | negative | vandalism_rollback | Temporary accounts and Dbdbah undo old revisions, reverted as "Vandalism" (RW) / "Rvv" by several patrollers. |
| frwiki | Électroéjaculation | 4 / 3 | uncertain | unclear | The frwiki exchange consists only of default "Annulation"/"Révocation" summaries, with no stated reason. |
| zhwiki | 李志 | 4 / 2 | positive | content_dispute | 12З4567 reverts 22 edits as advertising and AI editing; Xiliuwuxiang defends the sourcing (court judgment, birth-date dispute) and escalates to the talk page and admin board. |
| zhwiki | 聖公會聖馬利亞堂莫慶堯中學 | 4 / 3 | negative | patrol_tool | ChemicalCatalyst reverts temporary accounts with the AntiVandal script, and another patroller rolls back. |
