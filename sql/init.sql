
-- Load the extensions
create extension pg_cron;
create extension pgsalesforce;

-- Include the user space functions (mostly related to sync functionality)
\ir includes/pgsalesforce.sql

-- load env variables
\set SF_INSTANCE `echo "'$SF_INSTANCE'"`
\set SF_CLIENT_ID `echo "'$SF_CLIENT_ID'"`
\set SF_CLIENT_SECRET `echo "'$SF_CLIENT_SECRET'"`
\set SF_API_VERSION `echo "'$SF_API_VERSION'"`
\set SF_OBJECTS_TO_SYNC `echo "'$SF_OBJECTS_TO_SYNC'"`
\set SYNC_SCHEDULE `echo "'$SYNC_SCHEDULE'"`

-- store them as permanent GUCs (only superusers can do this)
alter system set pgsalesforce.instance to :SF_INSTANCE;
alter system set pgsalesforce.client_id to :SF_CLIENT_ID;
alter system set pgsalesforce.client_secret to :SF_CLIENT_SECRET;
alter system set pgsalesforce.api_version to :SF_API_VERSION;
alter system set pgsalesforce.objects_to_sync to :SF_OBJECTS_TO_SYNC;

-- set the GUCs for the current session
set pgsalesforce.instance to :SF_INSTANCE;
set pgsalesforce.client_id to :SF_CLIENT_ID;
set pgsalesforce.client_secret to :SF_CLIENT_SECRET;
set pgsalesforce.api_version to :SF_API_VERSION;
set pgsalesforce.objects_to_sync to :SF_OBJECTS_TO_SYNC;

-- check access to the Salesforce API
-- this will raise an error if credentials are invalid
\echo Checking access to the Salesforce API
select pgsalesforce.check_access_keys();

-- Declare the foreign data wrapper
\echo Creating the foreign data wrapper
create foreign data wrapper salesforce_fdw
handler salesforce_fdw_handler
validator salesforce_fdw_validator;

-- Create a server for Salesforce 
\echo Creating the server
select pgsalesforce.create_server(
    'my_salesforce_server', -- server_name
    'salesforce_fdw' -- fdw_name
);

\echo Creating local/foreign table pairs
call pgsalesforce.init_tables(
    'my_salesforce_server', -- server_name
    'salesforce', -- target_schema_name
    pgsalesforce.split_string_to_array(current_setting('pgsalesforce.objects_to_sync'), ',')
);

\echo Creating the cron job
select cron.schedule(:SYNC_SCHEDULE, $$
    call sync_data(
        'salesforce',
        pgsalesforce.split_string_to_array(current_setting('pgsalesforce.objects_to_sync'), ',')
    )
$$);

-- simple queries to run to check if everything is working
/*

select "Id", "FirstName", "LastName", "Company" from salesforce."Lead";
select "Id", "FirstName", "LastName", "Company" from salesforce."Lead_fdw";

insert into salesforce."Lead" ("FirstName", "LastName", "Company")
values ('John', 'Doe', 'Acme Inc')
returning "Id";

update salesforce."Lead"
set "Company" = 'Acme Corp'
where "Id" = 'XXXXXXXXXXXXXXXXX';

delete from salesforce."Lead"
where "Id" = 'XXXXXXXXXXXXXXXXX';

*/


-- Interesting queries to run, they work both on the local and foreign tables (add _fdw to the table name)

-- (Opportunity)

-- \echo 1. Total Amount by Stage
-- -- This query calculates the total amount of opportunities grouped by their stage.
-- SELECT "StageName", SUM("Amount") AS "TotalAmount"
-- FROM "salesforce"."Opportunity"
-- GROUP BY "StageName";

-- \echo 2. Average Opportunity Amount by Type
-- -- This query calculates the average amount of opportunities for each type.
-- SELECT "Type", AVG("Amount") AS "AverageAmount"
-- FROM "salesforce"."Opportunity"
-- GROUP BY "Type";

-- \echo 3. Top 5 Opportunities by Amount
-- -- This query ranks opportunities by amount and selects the top 5 highest.
-- -- Top 5 Opportunities by Amount
-- -- This query ranks opportunities by amount and selects the top 5 highest.
-- WITH RankedOpportunities AS (
--     SELECT "Id",
--            "Name",
--            "Amount",
--            RANK() OVER (ORDER BY "Amount" DESC) AS "Rank"
--     FROM "salesforce"."Opportunity_fdw"
-- )
-- SELECT "Id",
--        "Name",
--        "Amount",
--        "Rank"
-- FROM RankedOpportunities
-- WHERE "Rank" <= 5;


-- \echo 4. Cumulative Amount by Close Date
-- -- This query calculates the cumulative amount of opportunities over time.
-- SELECT "CloseDate",
--        SUM("Amount") OVER (ORDER BY "CloseDate" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "CumulativeAmount"
-- FROM "salesforce"."Opportunity";

-- \echo 5. Monthly Revenue Growth
-- -- This query calculates the monthly growth in the total amount of opportunities.
-- WITH "MonthlyTotals" AS (
--     SELECT DATE_TRUNC('month', "CloseDate") AS "Month",
--            SUM("Amount") AS "TotalAmount"
--     FROM "salesforce"."Opportunity"
--     GROUP BY DATE_TRUNC('month', "CloseDate")
-- )
-- SELECT "Month",
--        "TotalAmount",
--        LAG("TotalAmount") OVER (ORDER BY "Month") AS "PreviousMonthAmount",
--        "TotalAmount" - LAG("TotalAmount") OVER (ORDER BY "Month") AS "MonthlyGrowth"
-- FROM "MonthlyTotals";

-- \echo 6. Opportunities Created and Closed in Each Fiscal Quarter
-- -- This query counts the number of opportunities created and closed within each fiscal quarter.
-- SELECT "FiscalQuarter",
--        COUNT(CASE WHEN "CreatedDate" IS NOT NULL THEN 1 END) AS "CreatedCount",
--        COUNT(CASE WHEN "CloseDate" IS NOT NULL THEN 1 END) AS "ClosedCount"
-- FROM "salesforce"."Opportunity"
-- GROUP BY "FiscalQuarter";

-- \echo 7. Top Salesperson by Total Revenue
-- -- This query finds the top salesperson by total revenue. 
-- SELECT O."OwnerId",
--        S."Name" AS "SalespersonName",
--        SUM(O."Amount") AS "TotalRevenue"
-- FROM "salesforce"."Opportunity" O
-- JOIN "salesforce"."User" S ON O."OwnerId" = S."Id"
-- GROUP BY O."OwnerId", S."Name"
-- ORDER BY "TotalRevenue" DESC
-- LIMIT 1;

-- \echo 8. Opportunity Count and Average Amount by Account
-- -- This query counts the number of opportunities and calculates the average amount for each account.
-- SELECT "AccountId",
--        COUNT(*) AS "OpportunityCount",
--        AVG("Amount") AS "AverageAmount"
-- FROM "salesforce"."Opportunity"
-- GROUP BY "AccountId";

-- \echo 9. Forecast Category Analysis
-- -- This query counts the number of opportunities and calculates the total amount by forecast category.
-- SELECT "ForecastCategory",
--        COUNT(*) AS "OpportunityCount",
--        SUM("Amount") AS "TotalAmount"
-- FROM "salesforce"."Opportunity"
-- GROUP BY "ForecastCategory";

-- \echo 10. Opportunity Amount Trend by Month
-- -- This query analyzes the trend in the total amount of opportunities each month.
-- SELECT DATE_TRUNC('month', "CloseDate") AS "Month",
--        SUM("Amount") AS "TotalAmount"
-- FROM "salesforce"."Opportunity"
-- GROUP BY DATE_TRUNC('month', "CloseDate")
-- ORDER BY "Month";

-- -- (Leed)
-- \echo 11. Count of Leads by Status
-- -- This query counts the number of leads for each status.
-- SELECT "Status",
--        COUNT(*) AS "LeadCount"
-- FROM "salesforce"."Lead"
-- GROUP BY "Status";

-- \echo 12. Average Number of Employees by Industry
-- -- This query calculates the average number of employees for leads, grouped by industry.
-- SELECT "Industry",
--        AVG("NumberOfEmployees") AS "AverageEmployees"
-- FROM "salesforce"."Lead"
-- GROUP BY "Industry";

-- \echo 13. Top 5 Leads by Annual Revenue
-- -- This query ranks leads by annual revenue and selects the top 5.
-- WITH RankedLeads AS (
--     SELECT "Id",
--            "Name",
--            "AnnualRevenue",
--            RANK() OVER (ORDER BY "AnnualRevenue" DESC) AS "Rank"
--     FROM "salesforce"."Lead"
-- )
-- SELECT "Id",
--        "Name",
--        "AnnualRevenue",
--        "Rank"
-- FROM RankedLeads
-- WHERE "Rank" <= 5;

-- \echo 14. Leads Created and Converted Each Month
-- -- This query counts the number of leads created and converted each month.
-- WITH MonthlyStats AS (
--     SELECT DATE_TRUNC('month', "CreatedDate") AS "Month",
--            COUNT(*) FILTER (WHERE "IsConverted") AS "ConvertedCount",
--            COUNT(*) FILTER (WHERE "CreatedDate" IS NOT NULL) AS "CreatedCount"
--     FROM "salesforce"."Lead"
--     GROUP BY DATE_TRUNC('month', "CreatedDate")
-- )
-- SELECT "Month",
--        "CreatedCount",
--        "ConvertedCount"
-- FROM MonthlyStats;

-- \echo 15. Leads with the Most Recent Conversion Date
-- -- This query finds the most recent conversion date for leads that have been converted.
-- SELECT "Id",
--        "Name",
--        "ConvertedDate"
-- FROM "salesforce"."Lead"
-- WHERE "IsConverted"
-- ORDER BY "ConvertedDate" DESC
-- LIMIT 10;

-- \echo 16. Leads by Source and Status
-- -- This query counts the number of leads by lead source and status.
-- SELECT "LeadSource",
--        "Status",
--        COUNT(*) AS "LeadCount"
-- FROM "salesforce"."Lead"
-- GROUP BY "LeadSource", "Status";

-- \echo 17. Geographic Distribution of Leads
-- -- This query gets the average latitude and longitude of leads by country.
-- SELECT "Country",
--        AVG("Latitude") AS "AverageLatitude",
--        AVG("Longitude") AS "AverageLongitude"
-- FROM "salesforce"."Lead"
-- GROUP BY "Country";

-- \echo 18. Lead Conversion Rate by Lead Source
-- -- This query calculates the conversion rate for each lead source.
-- WITH LeadConversionStats AS (
--     SELECT "LeadSource",
--            COUNT(*) FILTER (WHERE "IsConverted") AS "ConvertedCount",
--            COUNT(*) AS "TotalCount"
--     FROM "salesforce"."Lead"
--     GROUP BY "LeadSource"
-- )
-- SELECT "LeadSource",
--        "ConvertedCount",
--        "TotalCount",
--        CASE WHEN "TotalCount" > 0 THEN ("ConvertedCount"::FLOAT / "TotalCount") * 100 ELSE 0 END AS "ConversionRate"
-- FROM LeadConversionStats;

-- \echo 19. Leads with Unread Status
-- -- This query counts the number of leads that are unread by the owner.
-- SELECT COUNT(*) AS "UnreadLeadCount"
-- FROM "salesforce"."Lead"
-- WHERE "IsUnreadByOwner";

-- \echo 20. Most Recent Lead by Each Owner
-- -- This query finds the most recent lead for each owner.
-- WITH MostRecentLeads AS (
--     SELECT "OwnerId",
--            "Id",
--            "Name",
--            "CreatedDate",
--            ROW_NUMBER() OVER (PARTITION BY "OwnerId" ORDER BY "CreatedDate" DESC) AS "RowNum"
--     FROM "salesforce"."Lead"
-- )
-- SELECT "OwnerId",
--        "Id",
--        "Name",
--        "CreatedDate"
-- FROM MostRecentLeads
-- WHERE "RowNum" = 1;
