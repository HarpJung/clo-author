# CLAUDE.MD -- Corporate Governance Research with Claude Code

<!-- HOW TO USE: Replace [BRACKETED PLACEHOLDERS] with your project info.
     Keep this file under ~150 lines — Claude loads it every session.
     See the guide at https://hugosantanna.github.io/clo-author/ for full documentation. -->

**Project:** Board Governance Systems: Directors, Advisors, and Observers
**Institution:** Harvard
**Branch:** main

---

## Core Principles

- **Plan first** -- enter plan mode before non-trivial tasks; save plans to `quality_reports/plans/`
- **Verify after** -- compile and confirm output at the end of every task
- **Single source of truth** -- Write-up `Paper/main.tex` is authoritative; presentation derives from it
- **Quality gates** -- weighted aggregate score; nothing ships below 80/100; see `quality.md`
- **Worker-critic pairs** -- every creator has a paired critic; critics never edit files
- **[LEARN] tags** -- when corrected, save `[LEARN:category] wrong → right` to MEMORY.md

---

## Research Question

**Core question:** How do the informal governance roles of board advisors and board observers complement, substitute for, or interact with formal board of directors oversight in corporate accountability?

**Gap:** Board of directors are extensively studied. Board advisors and board observers are prevalent in practice (PE/VC-backed companies, joint ventures, regulated industries) but under-documented in academic business literature. The *system* of all three roles as a governance mechanism is largely unexplored.

**Methodology:** Qualitative -- semi-structured interviews with 3 practitioners, supported by literature review and analysis of corporate governance documents.

**Deliverables:**
1. PowerPoint presentation -- research proposal (class)
2. ~10-page write-up (class)
3. (Future) Full journal article targeting accounting/finance/strategy/law journals

---

## Folder Structure

```
CorpAcct/
├── CLAUDE.MD                    # This file
├── .claude/                     # Rules, skills, agents, hooks
├── Bibliography_base.bib        # Centralized bibliography
├── Paper/                       # Main write-up (source of truth)
│   ├── main.tex                 # ~10-page class write-up
│   └── sections/                # Section-level .tex files
├── Presentation/                # PowerPoint presentation
│   └── proposal.pptx            # Research proposal presentation
├── Interviews/                  # Practitioner interview materials
│   ├── protocol/                # Interview protocol and question guide
│   ├── notes/                   # Interview notes and summaries
│   └── consent/                 # Consent forms (gitignored)
├── Figures/                     # Figures referenced in paper
├── Tables/                      # Tables referenced in paper
├── Preambles/header.tex         # LaTeX headers / shared preamble
├── master_supporting_docs/      # Reference papers and governance docs
├── quality_reports/             # Plans, session logs, reviews, scores
├── explorations/                # Research sandbox
└── templates/                   # Session log, quality report templates
```

---

## Commands

```bash
# Write-up compilation (3-pass, XeLaTeX)
cd Paper && TEXINPUTS=../Preambles:$TEXINPUTS xelatex -interaction=nonstopmode main.tex
BIBINPUTS=..:$BIBINPUTS bibtex main
TEXINPUTS=../Preambles:$TEXINPUTS xelatex -interaction=nonstopmode main.tex
TEXINPUTS=../Preambles:$TEXINPUTS xelatex -interaction=nonstopmode main.tex
```

---

## Quality Thresholds

| Score | Gate | Applies To |
|-------|------|------------|
| 80 | Commit | Weighted aggregate (blocking) |
| 90 | PR | Weighted aggregate (blocking) |
| 95 | Submission | Aggregate + all components >= 80 |
| -- | Advisory | Presentation (reported, non-blocking) |

See `quality.md` for weighted aggregation formula.

---

## Skills Quick Reference

| Command | What It Does | Relevance |
|---------|-------------|-----------|
| `/discover interview [topic]` | Literature review + research ideation | **Primary** -- map the lit gap |
| `/write [section]` | Draft write-up sections | **Primary** -- produce the write-up |
| `/review [file/--flag]` | Quality reviews | **Primary** -- review drafts |
| `/talk [mode] [format]` | Create presentations | Useful -- proposal presentation |
| `/strategize [question]` | Research design | Useful -- interview protocol |
| `/tools [subcommand]` | Utilities: compile, validate-bib, etc. | As needed |

---

## Research Pipeline

```
Phase 1: Literature Review
  Map what's known about board directors, advisors, observers
  Identify the gap (system view is under-studied)

Phase 2: Interview Design
  Semi-structured protocol for 3 practitioners
  Focus: how do these roles interact in practice?

Phase 3: Interview Execution
  Conduct and document 3 interviews

Phase 4: Synthesis
  Thematic analysis of interview findings
  Connect practice insights to literature gap

Phase 5: Write-up (~10 pages)
  Introduction, literature review, methodology, findings, discussion

Phase 6: Presentation
  PowerPoint research proposal
```

---

## Current Project State

| Component | File | Status | Description |
|-----------|------|--------|-------------|
| Literature Review | `../Board Roles -- Literature Review & Research Ideas.docx` | complete | Three-tier architecture: directors, observers, advisors |
| Interview 1 (Toj) | `../Interview with Toj.docx` | complete | Ex-hedge fund, VC/PE: distress, D&O, observers |
| Interview 2 (Matt) | `../Matt interviewe.md` | complete | VC founder: advisors, observer power, board lifecycle |
| Interview 3 (Fubini) | `../Fubini Interview Prep*.md` | complete | Ex-McKinsey, multi-board: cross-ownership comparison |
| Research Proposals | `../PhD Research Proposal Ideas.docx` | complete | 8 proposals from interviews + literature |
| Research Spec | `quality_reports/research_spec_board_governance.md` | complete | Formal specification synthesizing all inputs |
| Write-up | `Paper/main.tex` | not started | ~10-page class write-up |
| Presentation | `Presentation/proposal.pptx` | not started | PowerPoint research proposal |
