# OPA Properties EDA - Feature Analysis

## Data Overview

- Total properties: 581,068
- Source table: `musa5090s26-team5.core.opa_properties`
- All properties have a `sale_price` record

## Target Variable: `sale_price`

The model should predict `sale_price`, not `market_value`. Key considerations:

- **Dollar-sales** (`sale_price <= $1`): 154,862 properties (27%) — likely family transfers, exclude from model
- **Bundle sales** (multiple properties sold on the same date at the same price): should be excluded, as `sale_price` reflects the bundle total, not the individual property value
- **Valid sales after filtering**: ~329,795
- Median sale price: $57,900
- Mean sale price: $324,237 (heavily skewed by high-value outliers)

## Data Freshness

- Sales data goes back to 1700 (historical records are unreliable)
- Recommended training window: **sales since 2015** (157,700 records) for broader coverage
- Or **sales since 2020** (89,347 records) for stronger recency signal

## Recommended Features

### High Priority (low null rate, include directly)

| Feature | Null Count | Null Rate | Notes |
|---|---|---|---|
| `total_area` | 521 | <0.1% | Lot area in sq ft |
| `zip_code` | 157 | <0.1% | Good spatial proxy |
| `category_code` | 0 | 0% | Complete; use to filter to residential |
| `zoning` | 2,280 | 0.4% | Encode as categorical |

### Medium Priority (7-8% null rate, impute)

| Feature | Null Count | Null Rate | Notes |
|---|---|---|---|
| `total_livable_area` | 42,888 | 7.4% | Key size feature; impute with median by category |
| `year_built` | 42,888 | 7.4% | Derive `age` = current year - year_built |
| `exterior_condition` | 43,165 | 7.4% | Ordinal 1-7; impute with mode |
| `interior_condition` | 43,187 | 7.4% | Ordinal 1-7; impute with mode |

### Lower Priority (high null rate, use with caution)

| Feature | Null Count | Null Rate | Notes |
|---|---|---|---|
| `number_of_bedrooms` | 81,417 | 14% | Impute or drop |
| `number_of_bathrooms` | 86,579 | 14.8% | Impute or drop |

## Feature Engineering Plan

1. **`property_age`** — derive from `year_built`: `EXTRACT(YEAR FROM CURRENT_DATE) - year_built`
2. **`is_residential`** — binary flag from `category_code = 1`
3. **`sale_year`** / **`sale_recency`** — how many years ago the property was sold; use as a weighting factor
4. **Categorical encoding** — `zip_code`, `zoning`, `category_code`, `building_code_new` → one-hot or target encoding
5. **Condition score** — combine `exterior_condition` and `interior_condition` into a single averaged score

## Data Cleaning Plan

1. Exclude dollar-sales (`sale_price <= 1`)
2. Exclude bundle sales (same `sale_price` + same `sale_date` across multiple properties)
3. Filter to sales since 2015 for training
4. Filter to residential properties only (`category_code = 1`)
5. Impute nulls in medium-priority features with median (numeric) or mode (categorical)
6. Cap extreme outliers in `sale_price` (e.g., above 99th percentile) or use log transform
