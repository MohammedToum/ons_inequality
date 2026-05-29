Welcome to your new dbt project!

### Using the starter project

Try running the following commands:

- dbt run
- dbt test

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


## Layers

sources/seeds
  -> staging: clean and standardise raw ONS shapes
  -> intermediate: conform observations into current analytical records
  -> snapshots: keep SCD/history for facts
  -> marts/core dimensions: geography, time, data marking
  -> marts/core facts: user-facing analytical tables

## Flow

raw_ons sources
  -> stg_ons views
    -> int_ons views
      -> snapshots
        -> marts_ons fact tables


raw_regional_gdp_by_year
raw_regional_gdp_by_quarter
  -> stg_regional_gdp_by_year
  -> stg_regional_gdp_by_quarter
    -> int_regional_gdp_observations_current
      -> snap_fct_regional_gdp_observations
        -> fct_regional_gdp_observations
             joins dim_time_periods
             joins dim_time_quarters
             joins dim_geographies

raw_tax_benefits_statistics
  -> stg_tax_benefits_statistics
    -> int_tax_benefit_observations_current
      -> snap_fct_tax_benefit_observations
        -> fct_tax_benefit_observations
             joins dim_time_periods
             joins dim_geographies

raw_wellbeing_local_authority
raw_wellbeing_quarterly
  -> stg_wellbeing_local_authority
  -> stg_wellbeing_quarterly
    -> int_wellbeing_observations_current
      -> snap_fct_wellbeing_observations
        -> fct_wellbeing_observations
             joins dim_time_periods
             joins dim_time_quarters
             joins dim_geographies

raw_suicides_in_the_uk
  -> stg_suicides_in_the_uk
    -> int_suicide_observations_current
      -> snap_fct_suicide_observations
        -> fct_suicide_observations
             joins dim_time_periods
             joins dim_geographies


### Shared Dimesions:

seed_time_periods_lookup
  -> int_time_periods
    -> dim_time_periods
      -> dim_time_quarters

seed_local_authority_to_region_lookup
  -> int_local_authority_districts
    -> dim_local_authorities
    -> dim_geographies

seed_nation_lookup
  -> int_nations
    -> dim_nations
    -> dim_geographies

stg_regional_gdp_by_year
stg_regional_gdp_by_quarter
  -> dim_geographies   # adds observed GDP regions

seed_data_marking_values_lookup
  -> int_data_markings
    -> dim_data_marking
