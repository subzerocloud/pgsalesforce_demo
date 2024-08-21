# pgsalesforce FDW (Foreign Data Wrapper) for Salesforce
pgsalesforce is a PostgreSQL Foreign Data Wrapper (FDW) for Salesforce. It allows you to query Salesforce data from PostgreSQL. Using it's core functionality, a bi-directional data sync between Salesforce and PostgreSQL can be simply implemented using triggers (PG->Salesforce) and an UPSERT query (Salesforce->PG).
You can inspect (and customize) how the sync is done in [sql/includes/pgsalesforce.sql](sql/includes/pgsalesforce.sql).
The end result is a PostgreSQL instace (running in docker) that you can spin up and it will automatically create local tables for Salesforce objects and sync them with Salesforce.

## Features / Roadmap
- [x] Foreign Data Wrapper for Salesforce
- [x] Quals, Sorts, Limits pushed down to Salesforce
- [x] User-space PL/pgSQL functions/tirggers for setting up bi-directional sync
- [ ] Use bulk API for seeding data
- [ ] Use Streaming API in a background process for real-time sync (to reduce the number of API calls)

## Status
This project is in early development. It is not yet ready for production use. Use only on a sandbox/dev Salesforce instance.

## Setup

Create connected app in Salesforce and configure it (see [Configure a Connected App for the OAuth 2.0 Client Credentials Flow](https://help.salesforce.com/s/articleView?id=sf.connected_app_client_credentials_setup.htm&type=5) and [OAuth 2.0 Client Credentials Flow for Server-to-Server Integration](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials_flow.htm&type=5). The important parts when creating the connected app are:

- check "Enable OAuth Settings", "Enable for Device Flow" and "Enable Client Credentials Flow"
- add "Manage user data via APIs (api)" to the Selected OAuth Scopes
- After the app is created, click on "Manage" and then "Edit Policies", and in the "Client Credentials Flow", set the "Run As" to a user that has access to the objects you want to sync.
- Now go to the "View" of the app, click Consumer Key and Secret [Manage Consumer Details] on from the page that opens up, and copy the Consumer Key and Secret.
- rename .env.example to .env and fill in the values for `SF_INSTANCE`,`SF_CLIENT_ID`,`SF_CLIENT_SECRET`
- you can also test with curl if everything is working with the following commands:
```bash
# export env vars
source .env

# get access token
export SF_TOKEN=$( \
     curl -s -X POST https://$SF_INSTANCE/services/oauth2/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "grant_type=client_credentials" \
     -d "client_id=$SF_KEY" \
     -d "client_secret=$SF_SECRET" \
     | jq -r '.access_token'\
)
# store full uri in a variable
export SF_URI="https://$SF_INSTANCE/services/data/v61.0"

# get list of objects
curl -s -H "Authorization: Bearer $SF_TOKEN" $SF_URI/sobjects | jq '.sobjects[].name'

# run a query
curl -s -H "Authorization: Bearer $SF_TOKEN" $SF_URI/query/?q=SELECT+Id+FROM+Account | jq
```

## Running
```bash
# start the docker container
docker compose up -d

# connect to the database (the used env vars are in .env)
psql postgresql://$SUPER_USER:$SUPER_USER_PASSWORD@localhost/$DB_NAME
```

Check out [sql/init.sql](sql/init.sql) for a example of queries you can run:
```sql
-- run a select on the local table (synced data from Salesforce)
select "Id", "FirstName", "LastName", "Company" from salesforce."Lead";

-- run a select on the foreign table (live data from Salesforce)
select "Id", "FirstName", "LastName", "Company" from salesforce."Lead_fdw";

-- create a new lead in the local table (will be synced to Salesforce using the trigger)
insert into salesforce."Lead" ("FirstName", "LastName", "Company")
values ('John', 'Doe', 'Acme Inc')
returning "Id";
```


