# BigQuery raw table definitions inferred from the uploaded ONS CSV files.
#
# Use this with a google_bigquery_table resource, for example:
#
# resource "google_bigquery_table" "raw_tables" {
#   for_each            = local.raw_tables
#   dataset_id          = google_bigquery_dataset.raw.dataset_id
#   table_id            = each.value.table_id
#   schema              = jsonencode(each.value.schema)
#   deletion_protection = false
# }
#
# Notes:
# - Field names are sanitised to BigQuery-safe snake_case.
# - Original CSV column names are preserved in field descriptions where useful.
# - Raw landing tables are intentionally NULLABLE; enforce business rules in dbt tests/models.

locals {
  raw_tables = {
    regional_gdp_by_quarter = {
      table_id = "raw_regional_gdp_by_quarter"
      schema = [
        { name = "v4_1", type = "FLOAT", mode = "NULLABLE" },
        { name = "data_marking", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Data Marking" },
        { name = "yyyy_qq", type = "STRING", mode = "NULLABLE", description = "Original CSV column: yyyy-qq" },
        { name = "time", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Time" },
        { name = "nuts", type = "STRING", mode = "NULLABLE" },
        { name = "geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Geography" },
        { name = "sic_unofficial", type = "STRING", mode = "NULLABLE", description = "Original CSV column: sic-unofficial" },
        { name = "unofficial_standard_industrial_classification", type = "STRING", mode = "NULLABLE", description = "Original CSV column: UnofficialStandardIndustrialClassification" },
        { name = "type_of_prices", type = "STRING", mode = "NULLABLE", description = "Original CSV column: type-of-prices" },
        { name = "prices", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Prices" },
        { name = "quarterly_index_and_growth_rate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: quarterly-index-and-growth-rate" },
        { name = "growth_rate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: GrowthRate" },
      ]
    }

    regional_gdp_by_year = {
      table_id = "raw_regional_gdp_by_year"
      schema = [
        { name = "v4_1", type = "FLOAT", mode = "NULLABLE" },
        { name = "data_marking", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Data Marking" },
        { name = "calendar_years", type = "INTEGER", mode = "NULLABLE", description = "Original CSV column: calendar-years" },
        { name = "time", type = "INTEGER", mode = "NULLABLE", description = "Original CSV column: Time" },
        { name = "nuts", type = "STRING", mode = "NULLABLE" },
        { name = "geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Geography" },
        { name = "sic_unofficial", type = "STRING", mode = "NULLABLE", description = "Original CSV column: sic-unofficial" },
        { name = "unofficial_standard_industrial_classification", type = "STRING", mode = "NULLABLE", description = "Original CSV column: UnofficialStandardIndustrialClassification" },
        { name = "type_of_prices", type = "STRING", mode = "NULLABLE", description = "Original CSV column: type-of-prices" },
        { name = "prices", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Prices" },
        { name = "quarterly_index_and_growth_rate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: quarterly-index-and-growth-rate" },
        { name = "growth_rate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: GrowthRate" },
      ]
    }

    suicides_in_the_uk = {
      table_id = "raw_suicides_in_the_uk"
      schema = [
        { name = "v4_0", type = "INTEGER", mode = "NULLABLE" },
        { name = "calendar_years", type = "INTEGER", mode = "NULLABLE", description = "Original CSV column: calendar-years" },
        { name = "time", type = "INTEGER", mode = "NULLABLE", description = "Original CSV column: Time" },
        { name = "administrative_geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: administrative-geography" },
        { name = "geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Geography" },
      ]
    }

    tax_benefits_statistics = {
      table_id = "raw_tax_benefits_statistics"
      schema = [
        { name = "v4_0", type = "INTEGER", mode = "NULLABLE" },
        { name = "financial_and_calendar_years", type = "STRING", mode = "NULLABLE", description = "Original CSV column: financial-and-calendar-years" },
        { name = "time", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Time" },
        { name = "uk_only", type = "STRING", mode = "NULLABLE", description = "Original CSV column: uk-only" },
        { name = "geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Geography" },
        { name = "quintile", type = "STRING", mode = "NULLABLE" },
        { name = "quintile_2", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Quintile" },
        { name = "averages_and_percentiles", type = "STRING", mode = "NULLABLE", description = "Original CSV column: averages-and-percentiles" },
        { name = "averages_and_percentiles_2", type = "STRING", mode = "NULLABLE", description = "Original CSV column: AveragesAndPercentiles" },
        { name = "income_type", type = "STRING", mode = "NULLABLE", description = "Original CSV column: income-type" },
        { name = "income", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Income" },
        { name = "value_deflation", type = "STRING", mode = "NULLABLE", description = "Original CSV column: value-deflation" },
        { name = "deflation", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Deflation" },
      ]
    }

    wellbeing_local_authority = {
      table_id = "raw_wellbeing_local_authority"
      schema = [
        { name = "v4_3", type = "FLOAT", mode = "NULLABLE" },
        { name = "data_marking", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Data marking" },
        { name = "lower_limit", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Lower limit" },
        { name = "upper_limit", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Upper limit" },
        { name = "yyyy_yy", type = "STRING", mode = "NULLABLE", description = "Original CSV column: yyyy-yy" },
        { name = "time", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Time" },
        { name = "administrative_geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: administrative-geography" },
        { name = "geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Geography" },
        { name = "measure_of_wellbeing", type = "STRING", mode = "NULLABLE", description = "Original CSV column: measure-of-wellbeing" },
        { name = "measure_of_wellbeing_2", type = "STRING", mode = "NULLABLE", description = "Original CSV column: MeasureOfWellbeing" },
        { name = "wellbeing_estimate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: wellbeing-estimate" },
        { name = "estimate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Estimate" },
      ]
    }

    wellbeing_quarterly = {
      table_id = "raw_wellbeing_quarterly"
      schema = [
        { name = "v4_2", type = "FLOAT", mode = "NULLABLE" },
        { name = "lcl", type = "FLOAT", mode = "NULLABLE", description = "Original CSV column: LCL" },
        { name = "ucl", type = "FLOAT", mode = "NULLABLE", description = "Original CSV column: UCL" },
        { name = "yyyy_qq", type = "STRING", mode = "NULLABLE", description = "Original CSV column: yyyy-qq" },
        { name = "time", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Time" },
        { name = "uk_only", type = "STRING", mode = "NULLABLE", description = "Original CSV column: uk-only" },
        { name = "geography", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Geography" },
        { name = "measure_of_wellbeing", type = "STRING", mode = "NULLABLE", description = "Original CSV column: measure-of-wellbeing" },
        { name = "measure_of_wellbeing_2", type = "STRING", mode = "NULLABLE", description = "Original CSV column: MeasureOfWellbeing" },
        { name = "wellbeing_estimate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: wellbeing-estimate" },
        { name = "estimate", type = "STRING", mode = "NULLABLE", description = "Original CSV column: Estimate" },
        { name = "seasonal_adjustment", type = "STRING", mode = "NULLABLE", description = "Original CSV column: seasonal-adjustment" },
        { name = "seasonal_adjustment_2", type = "STRING", mode = "NULLABLE", description = "Original CSV column: SeasonalAdjustment" },
      ]
    }
  }
}
