-- this script was written and run in MySQL using the data contained in 'layoffs.csv' (available in the repository)

-- Data Cleaning of layoffs.csv for Exploratory Data Analysis

-- 1. Copy data to work off of
-- 2. Remove Duplicates (if any)
-- 3. Standardize the Data (spelling, formatting)
-- 4. Address Null Values or Blank values
-- 5. Remove Any Columns

-- 1. Copy data

SELECT *
FROM layoffs;

-- creating new table
CREATE TABLE layoffs_staging
Like layoffs;

SELECT *
FROM layoffs_staging;
-- columns have imported correctly

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;
-- new table looks great and is ready for cleaning

-- 2. Removing duplicates

-- Since there are no columns containing a unique id value, row_number is used and by partitioning by all of the columns 
-- in the sheet we can get a row_number count that will show if any of the rows are duplicate values
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- creates CTE using above table to filter on the row_num column so we can remove only the rows with a row_num greater than 1 (the duplicates)
WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num >1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';
-- this allows us to verify our previous query is working properly as we can see there is a duplicate entry in the results

-- this below statement will not successfully remove the duplicate rows because we cannot make modifications to CTEs
WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_CTE
WHERE row_num >1;
-- since  we cannot remove the rows from the CTE we will create a new table 

-- creates a table to import the data containing the row_num into so we can select can delete the rows with a row_num of 2 or more
-- created by copying a create statement from the layoffs_staging table
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- checking columns of new table for consistency
SELECT *
FROM layoffs_staging2;

-- inserting the data containing the row_number column
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- these are the duplicate rows to delete
SELECT *
FROM layoffs_staging2
WHERE row_num >1;

DELETE
FROM layoffs_staging2
WHERE row_num >1;
-- duplicate rows have been addressed

-- 3. Standardizing data

-- previous looks at the data showed very clear indications that some of the company cells contained leading or trailing spaces
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- replaces the company column after trimming trailing and leading spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

-- checking for industry name consistency
SELECT distinct industry
FROM layoffs_staging2
ORDER BY 1;
-- there are a few different entries all related to the cryptocurrency industry listed

-- filtering entries by industries that begin with 'crypto'
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';
-- the results show the vast majority of layoff entries are entered under the industry name 'Crypto' so we will update the other two to match for consistency

-- updating inconsistencies in names related to the 'Crypto' industry
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

-- check the industry column again
SELECT distinct industry
FROM layoffs_staging2
ORDER BY 1;

-- checking other columns for issues
SELECT DISTINCT country
FROM layoffs_staging2
order by 1;
-- there are two entries for the United States and one of them contains a trailing period

-- trailing allows us to trim other characters and strings from data entries besides spaces, as seen below
SELECT DISTINCT country, TRIM( TRAILING '.' FROM country)
FROM layoffs_staging2
order by 1;

-- updates any entries containing a trailing period by trimming and replacing
UPDATE layoffs_staging2
SET country = TRIM( TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- currently the date column is formatted as text strings
-- str_to_date is perfect here for converting the string data to the correct date formatting for MySQL
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- updating the date column with correctly formatted dates
UPDATE layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');
-- the strings are now in the correct date format but the column is still showing as text values

-- this will alter the data type of the column in our table
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- checking the layoffs_staging2 schema information confirms the date column is now the date data type

-- 4. NULL or Blank Values

SELECT *
FROM
layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- these entries will likely not be useful but will be addressed this in the next section

-- checks industry for NULLs and blanks (seen earlier)
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';
-- checking if there are any other layoff entries for the companies that have null and blank values may allow us to fill in the missing information

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
-- in this case, 1 other Airbnb entry contained the industry information (travel)

-- setting all blanks in the industry column to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- by joining the table on itself and filtering one for nulls + blanks and the other full non-nulls we are able 
-- to see which companies have multiple entries and can be used to fill in the missing industry data
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location -- in case of companies with multiple locations
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;
-- the resulting table shows the 3 companies that have multiple entries where one contains nulls/blanks in the industry column and the other does not

-- by converting the above query to an update statement, the null/blank entries shown in t1 can be updated where t2 has the industry data to replace the nulls/blanks in t1
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;
-- this query sets the t1 industry column = to the t2 industry column where t1 is null and t2 is not null

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
-- the blank industries that could be updated have now been updated

-- 5. Removing Columns and Rows

-- Since there is not enough information to populate total laid off or percentage laid off without knowing the total employees at the company before
--  the layoffs, these rows will be of little to no use and can be removed since we will be querying off of those columns often

SELECT *
FROM
layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- rows to be removed

DELETE
FROM
layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- the row_num column is also no longer necessary as it was only used for removing duplicates
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT*
FROM layoffs_staging2;

-- Now this data has been cleaned for the purposes of Exploratory Data Analysis
