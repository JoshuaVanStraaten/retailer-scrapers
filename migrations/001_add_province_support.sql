-- =============================================================================
-- Savvy Grocery - Database Migration
-- Version: 001
-- Description: Add province support and improve schema for multi-province scraping
-- =============================================================================

-- This migration:
-- 1. Adds the province column if it doesn't exist
-- 2. Adds normalized_name for better search
-- 3. Adds size_value and size_unit for filtering
-- 4. Adds parsed_at timestamp
-- 5. Creates a better primary key strategy using composite unique constraint
-- 6. Adds indexes for common query patterns

-- =============================================================================
-- Step 1: Add new columns
-- =============================================================================

-- Add province column
ALTER TABLE "Products"
ADD COLUMN IF NOT EXISTS province TEXT;

-- Add normalized_name for search
ALTER TABLE "Products"
ADD COLUMN IF NOT EXISTS normalized_name TEXT;

-- Add size extraction columns
ALTER TABLE "Products"
ADD COLUMN IF NOT EXISTS size_value DECIMAL;

ALTER TABLE "Products"
ADD COLUMN IF NOT EXISTS size_unit TEXT;

-- Add parsed timestamp
ALTER TABLE "Products"
ADD COLUMN IF NOT EXISTS parsed_at TIMESTAMPTZ DEFAULT NOW();

-- Add category if not exists
ALTER TABLE "Products"
ADD COLUMN IF NOT EXISTS category TEXT;


-- =============================================================================
-- Step 2: Update existing data (one-time backfill)
-- =============================================================================

-- Set default province for existing data (if not already set)
UPDATE "Products"
SET province = 'Gauteng'
WHERE province IS NULL OR province = '';

-- Generate normalized names for existing products
UPDATE "Products"
SET normalized_name = LOWER(REGEXP_REPLACE(name, '[^a-zA-Z0-9\s]', '', 'g'))
WHERE normalized_name IS NULL;

-- Set parsed_at for existing records
UPDATE "Products"
SET parsed_at = NOW()
WHERE parsed_at IS NULL;


-- =============================================================================
-- Step 3: Create unique constraint for upserts
-- =============================================================================

-- Drop old primary key if it's just the index column
-- Note: You may need to adjust this based on your actual schema
-- ALTER TABLE "Products" DROP CONSTRAINT IF EXISTS "Products_pkey";

-- Create a unique constraint on (name, retailer, province) for proper upserts
-- This allows the same product to exist in different provinces with different prices
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'products_name_retailer_province_unique'
    ) THEN
        ALTER TABLE "Products"
        ADD CONSTRAINT products_name_retailer_province_unique
        UNIQUE (name, retailer, province);
    END IF;
END $$;


-- =============================================================================
-- Step 4: Create indexes for performance
-- =============================================================================

-- Index for searching by retailer
CREATE INDEX IF NOT EXISTS idx_products_retailer
ON "Products" (retailer);

-- Index for searching by province
CREATE INDEX IF NOT EXISTS idx_products_province
ON "Products" (province);

-- Index for retailer + province (common filter)
CREATE INDEX IF NOT EXISTS idx_products_retailer_province
ON "Products" (retailer, province);

-- Index for full-text search on normalized name
CREATE INDEX IF NOT EXISTS idx_products_normalized_name
ON "Products" USING gin (to_tsvector('english', COALESCE(normalized_name, '')));

-- Index for price comparisons
CREATE INDEX IF NOT EXISTS idx_products_price_search
ON "Products" (retailer, province, name);

-- Index for recent updates
CREATE INDEX IF NOT EXISTS idx_products_parsed_at
ON "Products" (parsed_at DESC);


-- =============================================================================
-- Step 5: Create useful views
-- =============================================================================

-- View: Products with active promotions
CREATE OR REPLACE VIEW products_on_promotion AS
SELECT
    name,
    price,
    promotion_price,
    retailer,
    province,
    promotion_valid,
    image_url
FROM "Products"
WHERE promotion_price IS NOT NULL
  AND promotion_price != 'No promo'
  AND promotion_price != ''
ORDER BY retailer, name;


-- View: Price comparison across retailers (same product)
CREATE OR REPLACE VIEW price_comparison AS
SELECT
    name,
    array_agg(DISTINCT retailer) as retailers,
    array_agg(price) as prices,
    array_agg(province) as provinces,
    COUNT(DISTINCT retailer) as retailer_count
FROM "Products"
GROUP BY normalized_name, name
HAVING COUNT(DISTINCT retailer) > 1
ORDER BY retailer_count DESC, name;


-- View: Products by province
CREATE OR REPLACE VIEW products_by_province AS
SELECT
    province,
    retailer,
    COUNT(*) as product_count
FROM "Products"
GROUP BY province, retailer
ORDER BY province, retailer;


-- =============================================================================
-- Step 6: Create function for normalized name generation (for triggers)
-- =============================================================================

CREATE OR REPLACE FUNCTION generate_normalized_name()
RETURNS TRIGGER AS $$
BEGIN
    NEW.normalized_name := LOWER(REGEXP_REPLACE(NEW.name, '[^a-zA-Z0-9\s]', '', 'g'));
    NEW.parsed_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to auto-generate normalized_name on insert/update
DROP TRIGGER IF EXISTS trg_generate_normalized_name ON "Products";

CREATE TRIGGER trg_generate_normalized_name
BEFORE INSERT OR UPDATE ON "Products"
FOR EACH ROW
EXECUTE FUNCTION generate_normalized_name();


-- =============================================================================
-- Step 7: Create function for size extraction (optional enhancement)
-- =============================================================================

CREATE OR REPLACE FUNCTION extract_size_from_name(product_name TEXT)
RETURNS TABLE(size_val DECIMAL, size_unit_val TEXT) AS $$
DECLARE
    match_result TEXT[];
BEGIN
    -- Try to extract size like "500g", "1.5L", "2kg", etc.
    match_result := regexp_match(
        product_name,
        '(\d+(?:\.\d+)?)\s*(g|kg|ml|l|L|lt|mg)\b',
        'i'
    );

    IF match_result IS NOT NULL THEN
        size_val := match_result[1]::DECIMAL;
        size_unit_val := LOWER(match_result[2]);

        -- Normalize units
        IF size_unit_val = 'l' OR size_unit_val = 'lt' THEN
            size_val := size_val * 1000;
            size_unit_val := 'ml';
        ELSIF size_unit_val = 'kg' THEN
            size_val := size_val * 1000;
            size_unit_val := 'g';
        END IF;

        RETURN NEXT;
    ELSE
        size_val := NULL;
        size_unit_val := NULL;
        RETURN NEXT;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Update existing products with extracted sizes (one-time backfill)
UPDATE "Products" p
SET
    size_value = s.size_val,
    size_unit = s.size_unit_val
FROM (
    SELECT name, (extract_size_from_name(name)).*
    FROM "Products"
) s
WHERE p.name = s.name AND p.size_value IS NULL;


-- =============================================================================
-- Verification Queries (run manually to verify migration)
-- =============================================================================

-- Check column existence
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'Products';

-- Check indexes
-- SELECT indexname FROM pg_indexes WHERE tablename = 'Products';

-- Check constraints
-- SELECT conname FROM pg_constraint WHERE conrelid = 'Products'::regclass;

-- Sample data check
-- SELECT name, retailer, province, price, promotion_price, normalized_name
-- FROM "Products" LIMIT 10;

-- Province distribution
-- SELECT province, COUNT(*) FROM "Products" GROUP BY province;


-- =============================================================================
-- Rollback Script (if needed)
-- =============================================================================

-- To rollback this migration, run:
--
-- DROP TRIGGER IF EXISTS trg_generate_normalized_name ON "Products";
-- DROP FUNCTION IF EXISTS generate_normalized_name();
-- DROP FUNCTION IF EXISTS extract_size_from_name(TEXT);
-- DROP VIEW IF EXISTS products_on_promotion;
-- DROP VIEW IF EXISTS price_comparison;
-- DROP VIEW IF EXISTS products_by_province;
-- DROP INDEX IF EXISTS idx_products_retailer;
-- DROP INDEX IF EXISTS idx_products_province;
-- DROP INDEX IF EXISTS idx_products_retailer_province;
-- DROP INDEX IF EXISTS idx_products_normalized_name;
-- DROP INDEX IF EXISTS idx_products_price_search;
-- DROP INDEX IF EXISTS idx_products_parsed_at;
-- ALTER TABLE "Products" DROP CONSTRAINT IF EXISTS products_name_retailer_province_unique;
-- ALTER TABLE "Products" DROP COLUMN IF EXISTS province;
-- ALTER TABLE "Products" DROP COLUMN IF EXISTS normalized_name;
-- ALTER TABLE "Products" DROP COLUMN IF EXISTS size_value;
-- ALTER TABLE "Products" DROP COLUMN IF EXISTS size_unit;
-- ALTER TABLE "Products" DROP COLUMN IF EXISTS parsed_at;
-- ALTER TABLE "Products" DROP COLUMN IF EXISTS category;
