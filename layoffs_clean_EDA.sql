-- Exploratory Data Analysis of the cleaned world layoffs data from 2020-2023

SELECT *
FROM layoffs_staging2;
-- this is the cleaned table to be queried

-- investigating the largest layoffs and what % of the workforce it was
SELECT  Max(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;
-- the largest single layoff instance was 12k employees and the largest % of employees laid off was 100%, meaning, the business likely went under

-- investigating all instances where a company laid off 100% of their staff (percentage_laid_off = 1). using ORDER BY
-- on funds_raised_millions shows which companies had access to large amounts of funds and still laid off all of their staff
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- totaling all layoff entries by the stage the company is at in terms of funding and offering status
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;
-- Post-IPO is by far the largest portion of layoffs and these companies
-- tends to be very large companies who have public stock options (Amazon, Google, Facebook, etc.)

-- totaling all layoff entries by company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- Amazon had the largest number of total layoffs during this time period at 18150

-- totaling all layoff entries by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
-- The two hardest hit industries were Consumer and Retail
-- The two least affected industries who still had layoffs were Fin-Tech (financial technology) and Manufacturing

-- totaling all layoff entries by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
-- US had by far the most total laid off employees during this time, over 7 times the next highest country, India (256k vs 36k)

-- grouping the sum of total layoffs by date to find the largest single layoffs instance
SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 2 DESC;
-- 2023-01-04 had the highest # of single day layoffs with 16171

-- grouping the sum of total layoffs by year to see which year had the worst layoffs
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
-- 2022 had the most layoffs with a total of 160661
-- Of note, 2023 had 125677 layoffs but our table only contains data through March 6th, 2023, so it is likely 
-- 2023 will have a much higher number of layoffs than 2022 given this is only a little over 2 months of data

-- using substring to group by year and month
SELECT substring(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;
-- by turning this return into a CTE a rolling total column can be calculated to see the total layoffs progression over time

WITH Rolling_Total AS
(
SELECT substring(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_layoffs
, SUM(total_layoffs) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_Total;
-- the rolling total column created shows the sum of the previous total above and the new total layoffs to the left
-- the total # of layoffs from the rolling total is over 383k which we know from the look at layoffs by country are predominantly US based layoffs

-- here we look at the highest # of layoffs by year
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
ORDER BY 3 DESC;
-- unsurprisingly, the top layoffs each year are from large companies such as Google, Meta and Amazon

-- this query creates two CTEs, the first being the results of the above query and the second adds a ranking column and saves as another CTE to filter on the Rankings
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Rankings
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Rankings <= 5;
-- now rankings can be analyzed by year, in this case, the top 5 from each year by ranking


-- the above query can be modified to look at the other columns like below looking at the industries
WITH Industry_Year (industry, years, total_laid_off) AS
(
SELECT industry, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry,  YEAR(`date`)
), Industry_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Rankings
FROM Industry_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Industry_Year_Rank
WHERE Rankings <= 5;

-- The insights of this Exploratory Data Analysis show that there were a tremendously high number of people who were laid off during these 3 years, over 383,000 layoffs.
-- The vast majority of these layoffs occured in the United States (256,000) and the layoffs really ramp up towards the end of 2022, with no signs of slowing down in the 
-- beginning of 2023. 2022 has the highest number of layoffs but given how many occured within the small amount of data from the year 2023 (just over 2 months worth),
-- it looks to likely 2023 will far exceed the total number of layoffs from 2022. The hardest hit industries were the Counsumer, Retail, and Transportation. This could be
-- partially correlated to the COVID19 pandemic. Amazon laid off the largest total # of employees during this time (18,150) with Gooogle being the second highest. Google
-- also had the largest number of layoffs at a single time (12,000) with Meta right behind them. All of the largest numbers of layoffs were within large companies with
-- public IPOs such as Amazon, Google, Meta, Salesforce, Microsoft, Philips. In 2020 and 2021, the top 5 ranking companies in terms of layoffs included travel based businesses
-- such as Uber, Booking.com, Airbnb along with Groupon, Bytedance(Tiktok) and some delivery services. By 2022 and into 2023, the very large corporations have taken over the
-- list of the highest number of layoffs.

-- If you made it this far thank you for reading my code and explanations, I hope you found it useful and/or insightful.
