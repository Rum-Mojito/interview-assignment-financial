# Senior Data Engineer Interview Assignment - Financial Data Synthesizer

## Objective
This exercise evaluates your ability to design and implement a synthetic data generator for financial data. The system must support structured, categorical, numerical, and semi-structured fields (JSON/XML/text).

## Requirement 1 - Schema Generation
Input: A business data scenario (e.g., CRM system).

Output: Automatically generate a data schema or data model representing the system.

The schema should include:
- Multiple tables
- Primary and foreign keys
- Numerical fields- Categorical fields
- Semi-structured columns (JSON/XML/text)

## Requirement 2 - Synthetic Data Generation
Input: A given schema (SQLite, PostgreSQL, Parquet, or Delta Lake).Output: Generate synthetic data tables with a configurable number of records.
The generator should preserve:- realistic distributions- referential integrity- categorical relationships- semi-structured field structures

## Encouraged Libraries
itsz-ids/synthetic-data-generator (SDG)
The-FinAI/HARMONIC
CFA-Institute-RPC/Synthetic-Data-For-Finance
CTGAN / SDV frameworks
Faker or other synthetic data tools

## Expected Deliverables
Architecture design for the synthesizer tool
Schema generation approach
Synthetic data generation pipeline
Example generated dataset
Code implementation (Python preferred)
Explanation of how data distributions and relationships are preserved