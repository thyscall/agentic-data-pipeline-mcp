# Agent Interaction Log: [Feature Name]

> Document your experience building this feature with an AI agent. This log is a learning artifact, not a test. Be honest about what worked and what didn't.

---

## 1. Setup

**Agent tool used:** Cursor

**Why this tool?** I have access to a free year of it

**Date:** 4/13/2026

**Total time spent:** 4

---

## 2. Initial Specification

I need to build the Prefect flow for the web analytics data pipeline for Adventure works. 

The working data pipeline currently pulls sales data from PostgreSQL, chat logs from MongoDB, loads everything into Snowflake, and transforms it with dbt. That said, we are adding a new data source of web data that is found in a REST API, which is not currently integrated with the data warehouse. 

Before performing any action, read the following documents which are provided for explicit guidance and direction of how the build needs to implemented:

- @prefect/prd.md : Strict requirements, data mapping and schema, acceptance critera, scope, and questions/assumptions
- @prefect/api_docs.md: API schema and endpoint behavior

The action that you need to take is to write the complete, production-ready Prefect 2.0 flow inside the @web_analytics_flow.py file.

Follow the PRD with exactness, asking any follow up questions about the build needed to ensure that it is implemented as necessary. 

Be sure to follow the acceptance criteria, technical constraints, data schema, testing requirements, and out of scope sections with exactness. 

Now generate the plan and code for @prefect/flows/web_analytics_flow.py and the tests now.

**Did you share the PRD with the agent?** Yes. I included the PRD in the original chat using @prd.md for a direct reference to the file and location, essentially hyperlinking it for the agent

---

## 3. Iteration Log

### Iteration 1: [Brief description]
- **What I asked:** I gave the agent context about the project, Adventure Works, and what job I need to accmomplish with this data pipeline. 
I also directed the agent to look directly to the prd.md file and api_docs.md file that I created so that it had direct access to the correct context and information. 
- **What the agent produced:** The agent essentially "one-shotted" the code that was needed to accomplish the task. 
- **What worked:** The
- **What didn't work:** [Summary]
- **What I changed:** [Summary]

### Iteration 2: [Brief description]
- **What I asked:** I had linter errors in the web_analytics_flow.py and test files. These didn't impact the code's function. 
- **What the agent produced:** The agent implemented code to disable the error messages. 
- **What worked:** This was useful for clarity and transparency
- **What didn't work:** This was just a bandaid and didn't actually solve the linter errors
- **What I changed:** Nothing


---

## 4. Final Result

**Did the agent-generated code work on first run?** [Yes]

**If no, what broke?** 

**Percentage of final code written by the agent vs. you:**
- Agent wrote: [99]%
- I wrote/modified: [1]%

**Key files the agent created or modified:**
- [web_analytics_flow.py]: The agent wrote the complete pipeline, including API extraction, Pandas cleaning/casting, and Snowflake COPY INTO logic.
- [test_web_analytics_flow.py]: The agent generated the required unit tests as specified in the PRD.

---

## 5. What I Learned

### What the agent was good at:
- The agent was very efficient in writing lines of code after being given very specific instructions and context. 
- The agent was also good at understanding and resolving simple bugs like formatting errors, linter errors, libraries, and basic API infrastructure. 

### What the agent struggled with:
- The agent struggled with understanding file structure and locating variables.
- The agent also did not understand the full scope of the project and the implications of the end goal. 

### What I would do differently next time:
- I would gather more context around the scope of the project, what has already been completed this far, why, and how. 
- I would be more specific on the information of the Prefect flow view that I am looking for and similarly with Docker. 

### Time comparison estimate:
- **With agent:** [0.75 hours]
- **Without agent (estimate):** [8 hours]
- **Net impact:** Significantly faster due to the literal engine that will write the code, which changes my job to QA, infrastructure, and development design. 

---

## 6. Reflection

I was impressed by how efficient the Cursor agent was given the complex file structure and essentially jumping in mid project to accomplish the prefix flow task. I enjoyed the process of doing this because I had to zoom out and really understand what I wanted to happen, and then working backwards to accomplish that goal. This validated how I have previously used Cursor for product development. It is clear that there is a shift in software engineering of previously having to understand how to write every line of code character by character to now being able to guide the agent to get to the destination, accomplishing specific tasks along the way, and then ensuring that its work actually does work how it should. 
