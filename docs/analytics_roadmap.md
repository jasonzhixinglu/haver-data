# Analytics Roadmap: Monitoring, Nowcasting, and Agentic Analysis

*Last updated: 2026-04-17*

## 1. Motivation

The current pipeline provides reliable, portable access to Haver data via scheduled pulls and local parquet storage. This enables flexible analysis using Python and notebook workflows.

The next step is to build a unified framework on top of this pipeline to support:

- Real-time macroeconomic monitoring (e.g., China, G20 economies)
- Data-driven nowcasting workflows (e.g., DFM-based GDP nowcasts)
- Rapid exploratory analysis and charting
- AI-assisted (agentic) analysis and visualization

The goal is to move from raw data to structured insights in a consistent, reproducible, and flexible way.

---

## 2. Design Principles

### Separation of Concerns
- Data ingestion and storage are handled by this repository
- Analytics and modeling are built as a structured layer on top
- Agentic interaction is an interface to the analytics layer

### Flexibility over Rigid Dashboards
The system should expose general-purpose tools and primitives, not only pre-defined dashboards or charts.

### Reproducibility
All transformations and analytical steps should be explicit, traceable, and reusable.

### Metadata-Driven Design
Series selection and grouping should rely on metadata, tags, and standardized classifications (country, theme, frequency, use case).

### Composability
Higher-level workflows (e.g., dashboards, nowcasts) should be constructed from reusable building blocks.

---

## 3. Proposed Architecture

The extended system is organized into four layers:

### 3.1 Data Layer (Existing)
- Haver data pulled via API
- Stored as `data.parquet` and `metadata.parquet`
- GitHub used for synchronization and version control
- Tagging system for grouping and discovery

---

### 3.2 Access and Transformation Layer

Provides structured access to the raw data and common transformations.

**Core functionality:**
- Load data by:
  - series code
  - tag
  - country
  - theme
- Join with metadata
- Filter by date range
- Align frequencies (monthly, quarterly)

**Standard transformations:**
- Growth rates:
  - year-over-year (yoy)
  - month-over-month (mom)
  - quarter-over-quarter (qoq saar)
- Levels and rebasing
- Rolling averages
- Z-scores / standardization

This layer acts as the primary interface between raw data and all downstream analysis.

---

### 3.3 Analytics Layer

Provides reusable analytical tools and primitives.

#### Thematic Grouping
- Group series into themes:
  - activity
  - inflation
  - PMIs
  - trade
  - labor
  - financial conditions

#### Comovement and Summaries
- Principal component analysis (PCA)
- Diffusion / breadth indices
- Correlation structures

#### Factor Models
- Dynamic factor models (DFM)
- Latent activity indices
- Nowcasting inputs and outputs

#### Monitoring Metrics
- Momentum indicators (e.g., 3m/3m saar)
- Surprise measures relative to:
  - recent trends
  - model predictions
- Cross-series comparisons

#### Data Diagnostics
- Missing data patterns
- Last observation tracking
- Frequency consistency
- (Future) revision analysis with vintages

This layer should function as a **toolbox**, not a fixed dashboard.

---

### 3.4 Interface Layer

Provides user-facing outputs and interaction modes.

#### Charting
- Standard chart templates:
  - line charts (single and multi-series)
  - panel dashboards
  - contribution charts
  - heatmaps
  - diffusion indices
- Consistent labeling and formatting using metadata

#### Monitoring Dashboards
- Country-specific dashboards (e.g., China, Japan)
- Theme-based panels (activity, inflation, PMIs)
- Automatically updated with latest data

#### Nowcasting Workflows
- Dataset construction from tagged series
- Model execution (e.g., DFM)
- Visualization of:
  - forecast updates
  - contributions
  - revisions

#### Agentic Interface
- AI-assisted tools for:
  - custom chart generation
  - exploratory analysis
  - data summaries

The agent interacts with the system through structured tools rather than raw data access.

---

## 4. Example Workflows

### 4.1 Monitoring Dashboard (China)

1. Load activity-related series using tags
2. Apply yoy transformations and smoothing
3. Compute a PCA-based activity index
4. Generate a multi-panel chart with key indicators

---

### 4.2 G20 Nowcasting

1. Load series tagged for nowcasting inputs
2. Construct a model-ready dataset
3. Run DFM update
4. Visualize forecast changes and contributions

---

### 4.3 Exploratory Analysis

User query:
"Compare recent inflation dynamics in China and Korea"

System:
1. Retrieve relevant inflation series via tags
2. Apply transformations (yoy, rolling averages)
3. Generate comparison chart
4. Summarize recent trends

---

### 4.4 Agentic Charting

User query:
"Show a summary of China activity indicators over the past 2 years"

System:
1. Load China activity theme
2. Standardize and aggregate indicators
3. Generate panel chart
4. Provide brief narrative summary

---

## 5. Tagging System

Tags are central to enabling flexible data access and analysis.

### Current Use
- Tagging for nowcasting inputs (e.g., `gdp_nowcast`)

### Proposed Extensions
- Country:
  - `country:china`, `country:japan`
- Theme:
  - `theme:activity`, `theme:inflation`, `theme:pmi`, `theme:trade`
- Use case:
  - `use:monitoring`, `use:nowcast`, `use:chartbook`
- Frequency:
  - `freq:monthly`, `freq:quarterly`

### Benefits
- Dynamic dataset construction
- Consistent grouping across workflows
- Efficient agent-driven retrieval

---

## 6. Vintage Data (Future Extension)

For real-time analysis and nowcasting evaluation, the system may be extended to store historical vintages.

### Options
- Snapshot-based storage:
  - `data/snapshots/YYYY-MM-DD/`
- Long format with pull date:
  - `date, code, value, pull_date`

### Applications
- Revision analysis
- Pseudo-real-time nowcasting
- Release impact tracking

---

## 7. Future Extensions

- Automated data quality checks and alerts
- Config-driven dashboards and chartbooks
- Integration with external data (e.g., consensus forecasts)
- Narrative generation for monitoring reports
- Cross-country comparative frameworks

---

## 8. Phase 1 Implementation Plan

The first phase focuses on building a minimal but scalable analytics layer on top of the existing data pipeline.

### 8.1 Access Layer
- Extend `load.py` into a cached access module
- Add helpers for:
  - loading by tag, country, and theme
  - metadata joins
  - date filtering and frequency alignment

### 8.2 Transformation Utilities
- Implement standard transformations:
  - yoy, mom, qoq saar
  - rolling means
  - rebasing and z-scores
- Ensure consistent handling of missing data and frequencies

### 8.3 Tagging Expansion
- Extend the tag system to include:
  - country
  - theme
  - use case
  - frequency
- Update configuration to reflect structured tagging

### 8.4 Monitoring Packs
- Define curated indicator sets for:
  - China monitoring (activity, inflation, PMIs)
  - G20 nowcasting inputs
- Store as code or config-based definitions

### 8.5 Chart Templates
- Implement a small set of reusable chart templates:
  - multi-series line charts
  - panel dashboards
  - simple heatmaps or summary tables

### 8.6 Basic Agent Tools (Optional Initial Step)
- Define a minimal set of callable functions:
  - data retrieval
  - transformation
  - chart generation
- Enable simple prompt-driven chart creation

---

## 9. Summary

This roadmap extends the current Haver data pipeline into a flexible and scalable analytics framework.

The key objective is to combine:
- reliable data access
- reusable analytical tools
- structured workflows
- and flexible, agent-assisted interaction

into a unified system for macroeconomic monitoring, analysis, and forecasting.
