# Agent Data Access Reflection

> Now that you've set up the dbt MCP server and seen an AI agent interact with your data models, take some time to think critically about what this means for data engineering. This reflection should be thoughtful (500-800 words), not a checklist.

---

## 1. What Worked Well

_Think about the experience of exposing your dbt models to an agent via MCP. What was effective? What surprised you about how the agent interacted with your data?_

Exposing my dbt models to an agent via MCP was effective because of the context and explicit data mapping that was performed after the data was staged in Snowflake. The agent was able to navigate relationships between models because of the business rules and clear descriptions in the YML files which included the grain and connections required for each data type. I was surprised how straightforward the MCP creation and connection was once the dbt project was created. 
---

## 2. What Was Difficult or Confusing

_Where did the agent struggle? What required manual intervention or clarification? What parts of the setup were harder than expected?_

The most confusing part was dealing with limited visibility into Docker and the pickiness of the MCP server. Running the server in a Docker container made it difficult to see what was happening behind the scenes which added extra steps to debugging as opposed to running a local script. I also saw how unforgiving APIs and agents are. I passed "model" into demo_client.py to output all dbt models and it returned an error because if was not formatted as a list ["model]. Data types make or break the scripts used for agentic architectures (quite literally).

---

## 3. Documentation Quality

_You enhanced your dbt model and column descriptions to be "agent-friendly." Reflect on that process. What did you change? Why?_

My original dbt model and column descriptions were too vague. A human could infer what a customer_id may be, but an agent does not have the context to make that inference. My descriptions changed from simply describing what the customer_id represented to giving granular explanations of what the data type was, what purpose it has, and what actions to take using the data type like joining it with stg_ecom__email_campaigns.customer_id and int_web_analytics_with_customers.customer_id to attach customer attributes to events and transactions. The new descriptions eliminated ambiguity for both human engineers and agents. 

---

## 4. Production Considerations

_Imagine deploying this MCP server in a real company. What changes would be needed? What risks would you need to address?_

Deploying this in production would need strict guardrails regarding security, trust, and cost. Snowflake warehouse timeouts could be used so that the agent does not burn through budget with a poor query. The agent would need to have its own role in Snowflake with restricted permissions and a restricted dbt profile so that the agent can't see the sensitive data even if it hallucinates and tries. Another option would be to dynamically mask the sensitive data. The production system should also return logs of what queries were used to draw conclusions so they can be trusted and verified. 

---

## 5. Business Use Cases

_Based on what you've seen, describe 2-3 realistic business use cases where an agent with MCP access to a data warehouse could provide value._

Use Case 1: Revenue Attribution
Dashboards provide static metrics and stakeholders want to know the why and how behind it. They ask questions like "How much of this year's revenue can be attributed to the marketing event in SLC?" This would take an analyst hours whereas the stakeholder could ask the agent that question and it could reason through an accurate answer quickly because it is connected to the database via MCP. 

Use Case 2: Behavioral Analysis
Purchase orders and web traffic often live in two different places and don't talk to each other. It's common for a Product Manager to ask, "Which product pages have the highest traffic but lowest purchase rate?" The agent could identify the bottleneck by reading the MCP descriptions and joining the raw API data with the purchase data from PostgreSQL. 

---

## 6. The Bigger Picture

_Data engineers have traditionally built pipelines that serve dashboards and reports. With agent access layers like MCP, data engineers are now also responsible for making data accessible to AI agents. How does this change the role?_

Data engineers have focused on getting data from Point A to Point B through traditional ETL processes, but with agent and MCP, the role fundamentally shifts. "Agent-friendly" data modeling requires a much deeper understanding of what the data means for the business. For example, when a data engineer builds a dashboard, the logic and queries are hardcoded, but when an AI agent is given access to the database, the agent can write any query it wants. This requires the engineer to build a semantic layer and verify the accuracy of the data and queries behind the dashboard. Communication and context are now critical responsibilities. Data engineers are no longer just delivering data, but rather building the source of truth that AI agents will use to support business decision making.