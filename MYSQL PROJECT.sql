-- DATA CLEANING


SELECT * 
FROM layoffs;

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- 1. Remove Duplicates

# First let's check for duplicates

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM layoffs_staging;

SELECT * ,
ROW_NUMBER() OVER(PARTITION BY COMPANY,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`) AS ROW_NUM
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,
PERCENTAGE_LAID_OFF,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS) AS ROW_NUM
FROM layoffs_staging
)
SELECT *
FROM DUPLICATE_CTE
WHERE ROW_NUM > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!

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


SELECT * 
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,
PERCENTAGE_LAID_OFF,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS) AS ROW_NUM
FROM layoffs_staging;


SELECT *
FROM layoffs_staging2;


-- 2. STANDARDIZING DATA

SELECT COMPANY,TRIM(COMPANY)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET COMPANY = TRIM(COMPANY);

SELECT *
FROM layoffs_staging2
WHERE INDUSTRY LIKE 'CRYPTO%';

UPDATE layoffs_staging2
SET INDUSTRY = 'Crypto'
WHERE industry like 'crypto%';



select *
from layoffs_staging2
where country like 'united States%'
order by 1;


SELECT DISTINCT COUNTRY, TRIM(TRAILING '.' FROM COUNTRY)
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2 
SET COUNTRY = TRIM(TRAILING '.' FROM COUNTRY)
WHERE COUNTRY LIKE 'united States%';


SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM LAYOFFs_STAGING2;



UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE INDUSTRY IS NULL
OR INDUSTRY = '';


SELECT T1.industry,T2.industry
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.COMPANY = T2.COMPANY
WHERE T1.industry IS NULL 
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.COMPANY = T2.COMPANY
SET T1.INDUSTRY = T2.INDUSTRY
WHERE T1.industry IS NULL 
AND T2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

alter table layoffs_staging2
drop column row_num;