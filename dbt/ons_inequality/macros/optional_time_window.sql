?{#
    Adds optional year-bound filters for batch dbt runs.

    Usage from a model:
        where 1 = 1
        {{ optional_year_window('calendar_year') }}

    With --vars '{start_year: 2020, end_year: 2022}', this renders:
        and calendar_year >= 2020
        and calendar_year <= 2022

    If either var is missing or null, that side of the filter is skipped.
    If both vars are missing/null, the macro renders nothing and the model
    returns the full dataset.

    For period rows that span two years, pass both columns:
        {{ optional_year_window('period_start_year', 'period_end_year') }}
    That renders lower-bound filtering against the start column and upper-bound
    filtering against the end column.

    For financial-year rows that should be batched by reporting/end year, pass
    the end-year column only:
        {{ optional_year_window('end_year') }}
    With --vars '{start_year: 2021, end_year: 2021}', this includes rows whose
    financial year ends in 2021, such as 2020-21.
#}
{% macro optional_year_window(start_column, end_column=none) -%}
    {%- set start_year = var('start_year', none) -%}
    {%- set end_year = var('end_year', none) -%}
    {%- set resolved_end_column = end_column if end_column is not none else start_column -%}

    {%- if start_year is not none %}
      and {{ start_column }} >= {{ start_year }}
    {%- endif %}
    {%- if end_year is not none %}
      and {{ resolved_end_column }} <= {{ end_year }}
    {%- endif %}
{%- endmacro %}

{#
    Adds optional year/quarter-bound filters for quarterly batch dbt runs.

    Usage from a model:
        where 1 = 1
        {{ optional_quarter_window('year', 'quarter') }}

    With --vars '{start_year: 2020, start_quarter: 2, end_year: 2021, end_quarter: 3}',
    this renders boundary-aware logic equivalent to:
        2020 Q2 through 2021 Q3

    This intentionally does not use:
        year between 2020 and 2021
        and quarter between 2 and 3

    That pattern is wrong across multi-year ranges because it applies the
    quarter bounds to every year instead of only the start and end years.

    If start_quarter/end_quarter are missing, the macro falls back to simple
    year bounds. If all vars are missing/null, it renders nothing.
#}
{% macro optional_quarter_window(year_column, quarter_column) -%}
    {%- set start_year = var('start_year', none) -%}
    {%- set end_year = var('end_year', none) -%}
    {%- set start_quarter = var('start_quarter', none) -%}
    {%- set end_quarter = var('end_quarter', none) -%}

    {%- if start_year is not none and start_quarter is not none %}
      and (
        {{ year_column }} > {{ start_year }}
        or (
          {{ year_column }} = {{ start_year }}
          and {{ quarter_column }} >= {{ start_quarter }}
        )
      )
    {%- elif start_year is not none %}
      and {{ year_column }} >= {{ start_year }}
    {%- endif %}

    {%- if end_year is not none and end_quarter is not none %}
      and (
        {{ year_column }} < {{ end_year }}
        or (
          {{ year_column }} = {{ end_year }}
          and {{ quarter_column }} <= {{ end_quarter }}
        )
      )
    {%- elif end_year is not none %}
      and {{ year_column }} <= {{ end_year }}
    {%- endif %}
{%- endmacro %}
