"""Compare NVCA Model IRA 2023 vs 2025 observer provisions."""

print("=" * 80)
print("NVCA MODEL IRA: OBSERVER PROVISION COMPARISON (2023 vs 2025)")
print("=" * 80)

print("""
=== 2023 VERSION (Para 136) ===

  As long as [_____] owns not less than [____%] of the shares of the
  Preferred Stock it is purchasing under the Purchase Agreement] (or an
  equivalent amount of Common Stock issued upon conversion thereof)
  (as adjusted for stock splits, stock combinations, stock dividends
  and the like), the Company shall invite a representative of such
  Investor to attend all meetings of the Board of Directors in a
  nonvoting observer capacity and, in this respect, shall give such
  representative copies of all notices, minutes, consents, and other
  materials that it provides to its directors [at the same time and in
  the same manner as provided to such directors]; provided, however,
  that such representative shall agree to hold in confidence all
  information so provided; and provided further, that the Company
  reserves the right to withhold any information and to exclude such
  representative from any meeting or portion thereof if access to such
  information or attendance at such meeting would be reasonably likely
  to adversely affect the attorney-client privilege between the Company
  and its counsel or result in disclosure of trade secrets or highly
  confidential information or create a conflict of interest, or if such
  Investor or its representative is a Competitor.]


=== 2025 VERSION (Para 134) ===

  As long as [_____] owns not less than [____] shares of Preferred Stock
  (as adjusted for stock splits, stock combinations, stock dividends and
  the like), the Company shall invite a representative of such Investor
  to attend all meetings of the Board of Directors in a nonvoting observer
  capacity and, in this respect, shall give such representative copies of
  all notices, minutes, consents, and other materials that it provides to
  the Board of Directors [promptly following provision to the directors];
  provided, however, that such representative shall agree to hold in
  confidence all information so provided; and provided further, that the
  Company reserves the right to withhold any information and to exclude
  such representative from any meeting or portion thereof if (x) access
  to such information or attendance at such meeting would be reasonably
  likely to adversely affect the attorney-client privilege between the
  Company and its counsel, result in disclosure of trade secrets or
  highly confidential information, or create a competitive harm or
  competitive disadvantage, or (y) the portion as to which such
  representative is excluded relates solely to subject matter in which
  the Investor or such representative may have a conflict of interest
  [, or (z) if such Investor or its representative is a Competitor].]
""")

print("=" * 80)
print("KEY DIFFERENCES (6 changes identified)")
print("=" * 80)

print("""
CHANGE 1: Ownership threshold format
  2023: "[____%] of the shares of the Preferred Stock it is purchasing
        under the Purchase Agreement] (or an equivalent amount of Common
        Stock issued upon conversion thereof)"
  2025: "[____] shares of Preferred Stock"

  >> Simplified from percentage-of-series to absolute share count.
     Less protective for investors -- absolute count does not
     automatically adjust for dilution the way a percentage does.


CHANGE 2: Material delivery timing
  2023: "[at the same time and in the same manner as provided to
        such directors]"
  2025: "[promptly following provision to the directors]"

  >> WEAKENED observer access. "Promptly following" allows a delay
     between when directors get materials and when observers get them.
     In 2023, observers got materials AT THE SAME TIME as directors.
     This is significant -- even a few hours' delay means directors
     can discuss materials before observers see them.


CHANGE 3: Competitive harm carve-out (NEW in 2025)
  2023: Company can exclude observer for "conflict of interest"
  2025: Company can exclude for "competitive harm or competitive
        disadvantage" (clause x) -- SEPARATE from conflict of interest

  >> EXPANDED exclusion grounds. "Competitive harm or competitive
     disadvantage" is BROADER than "conflict of interest." Companies
     can now exclude observers simply because the observer's VC has
     portfolio companies that MIGHT benefit from the information --
     even without a personal conflict of interest. This directly
     addresses the DOJ/FTC Clayton Act concern.


CHANGE 4: Structured exclusion grounds
  2023: Single proviso with "or" connectors (unstructured list)
  2025: Structured as (x), (y), (z) sub-clauses

  >> Clearer structure makes it EASIER for companies to invoke
     specific exclusion grounds. Each sub-clause is independently
     sufficient. Three distinct bases for excluding an observer:
     (x) attorney-client/trade secret/competitive harm
     (y) conflict of interest on specific subject matter
     (z) investor is a Competitor


CHANGE 5: Conflict of interest becomes narrower but separate
  2023: "create a conflict of interest" (lumped with other exclusions)
  2025: Clause (y): "the portion as to which such representative is
        excluded relates solely to subject matter in which the Investor
        or such representative may have a conflict of interest"

  >> The "relates solely to subject matter" language NARROWS when
     conflict-of-interest exclusion applies (only for specific agenda
     items, not the whole meeting). But having it as a separate clause
     (y) makes it easier to invoke alongside (x) competitive harm.


CHANGE 6: Competitor definition expanded safe harbor
  2023: "shall not include any financial investment firm"
  2025: "shall not include any financial investment firm or collective
        investment vehicle that, together with its Affiliates, holds
        less than 20% of the outstanding equity of any Competitor and
        does not, nor do any of its Affiliates, have a right to
        designate any members of, or observers to, the board of
        directors"

  >> EXPANDED safe harbor for VC funds. A VC holding <20% in a
     competitor AND without board/observer seats at that competitor
     is explicitly NOT a Competitor. This directly addresses Clayton
     Act Section 8 concerns -- allows VCs to hold minority stakes
     in competing companies without losing observer rights elsewhere,
     AS LONG AS they don't have board/observer seats at the competitor.
""")

print("=" * 80)
print("WHAT STAYED THE SAME")
print("=" * 80)

print("""
  - Observer still has "nonvoting observer capacity" (unchanged)
  - Observer still receives "copies of all notices, minutes, consents,
    and other materials" (unchanged)
  - Observer must "agree to hold in confidence all information" (unchanged)
  - NO fiduciary language in EITHER version (removed in 2020)
  - Attorney-client privilege exclusion (unchanged)
  - Trade secrets exclusion (unchanged)
  - Competitor exclusion option [bracketed] (unchanged)
""")

print("=" * 80)
print("TIMELINE OF THREE NVCA SHOCKS")
print("=" * 80)

print("""
  Pre-2020:  Observer must act "in a fiduciary manner"
             --> Behavioral anchor toward responsible governance

  2020:      REMOVED fiduciary language
             --> Weakened observer OBLIGATIONS (freed from duty)

  Jan 2025:  DOJ/FTC Clayton Act statement on observers
             --> External regulatory pressure on observer interlocks

  Oct 2025:  EXPANDED company exclusion rights for observers
             + DELAYED material delivery timing
             + NEW "competitive harm" exclusion ground
             + Expanded Competitor safe harbor for VCs
             --> Restricted observer INFORMATION ACCESS
             --> Simultaneously protected VC observer RIGHTS

  NARRATIVE: 2020 freed observers from obligations.
             2025 restricts what information they can access.
             These are OPPOSITE DIRECTIONS on different channels:
               2020 = accountability channel (weaker)
               2025 = information channel (restricted)

  RESEARCH IMPLICATION: This creates a DOUBLE-SHOCK design:
    Shock 1 (2020): Does removing accountability worsen governance?
    Shock 2 (2025): Does restricting information access reduce leakage?
    The two shocks test DIFFERENT aspects of the accountability-
    information tradeoff at the heart of the paper.
""")
