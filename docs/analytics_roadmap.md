# Analytics Roadmap: Monitoring and Analysis Layer

*Last updated: 2026-04-17*

## 1. Motivation

The current pipeline provides reliable access to Haver data in a portable parquet format, making it easy to work with large sets of macroeconomic time series.

The next step is to build a layer on top of this data that supports ongoing workflows such as:
- G20 GDP nowcasting
- Country-level monitoring (e.g., China, Japan)
- Internal analytical products (e.g., quarterly inflation monitor)

The goal is to make it straightforward to move from raw data to structured analysis and charts, using a consistent and reusable framework.

---

## 2. Layered Structure

The system can be viewed as three layers:

### Data Layer (existing)
- Haver data ingestion and storage
- Parquet datasets and metadata
- Tagging for organizing and retrieving series

### Monitoring and Analysis Layer (in development)
- Structured access to data by country, theme, and use case
- Standard transformations (e.g., yoy, qoq, smoothing)
- Reusable analytical components (e.g., grouped indicators, simple summaries, model inputs)
- Direct support for monitoring and nowcasting workflows

### Agentic Layer (future)
- Flexible interface for querying data and generating charts
- Prompt-driven analysis built on top of the monitoring and analysis layer
- Designed to enable rapid, custom exploration without direct handling of raw data

---

## 3. Current Focus

The immediate goal is to build out the **monitoring and analysis layer**.

This involves creating a set of reusable tools and structured datasets that sit between raw data and downstream applications, so that core workflows can be run in a consistent and scalable way.

---

## 4. Core Use Cases

### G20 GDP Nowcasting
- Construct model-ready datasets from tagged input series
- Integrate with existing DFM nowcasting frameworks
- Track updates and contributions as new data arrive

### Country Monitoring Dashboards (China, Japan)
- Organize indicators into themes (activity, inflation, PMIs, etc.)
- Apply consistent transformations and smoothing
- Generate regular charts summarizing current economic conditions

### Inflation Monitor (Quarterly)
- Assemble cross-source inflation indicators
- Produce standardized analytical summaries
- Support recurring internal reporting with consistent data inputs

---

## 5. Initial Build-Out

The first phase of development will focus on:

- Extending data access to support loading by tag, country, and theme
- Implementing a small set of standard transformations (yoy, qoq, rolling averages)
- Defining curated indicator sets for key use cases
- Connecting these datasets to existing nowcasting and monitoring workflows
- Adding simple, reusable charting functions for quick visualization

---

## 6. Summary

This roadmap focuses on building a monitoring and analysis layer that bridges raw Haver data and applied macroeconomic workflows.

The emphasis is on supporting real use cases with a small set of reusable tools, while leaving room for more flexible, agent-assisted analysis in a later stage.
