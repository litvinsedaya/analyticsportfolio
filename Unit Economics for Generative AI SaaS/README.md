# Case 1: Unit Economics for Generative AI SaaS  

**Outcome:** Reduced GPU infrastructure costs by **30%** through predictive autoscaling and accurate cost allocation per generation.

❗️For confidentiality reasons, the outputs shown in the notebook are either anonymized or mocked, but the queries and analysis logic remain intact.

---

## Problem  
The company operated a Generative AI SaaS platform for designers. Users generated content with different AI models, some of which ran on our own GPU nodes rather than third-party APIs.  

The legacy pricing model gave each plan a fixed number of generations on a subset of models. This caused:  
- Low conversion from freemium to paid,  
- High early churn,  
- Negative feedback.  

Management decided to move to a **credit-based model**, where plans differ only in credits, and users can spend them on any model.  
To support this, I had to calculate the **true unit cost per generation in USD**, covering both active and idle infrastructure costs.  

---

## Approach  

1. **Node Cost Allocation**  
   - Parsed cron schedules for GPU node pools (`a100`, `l4`).  
   - Applied flat hourly rates to node uptime and distributed costs per day and hour.  
   - Used SQL time series (`generate_series`) to align schedules with calendar days.  

2. **Task Duration & Logs**  
   - Extracted generation tasks (1 task = 1 generation) and computed duration from status logs.  
   - Allocated daily node costs across completed tasks.  
   - Produced a **weighted average $/generation**, accounting for idle capacity.  

3. **Idle Time Discovery**  
   - Compared node availability with actual task execution.  
   - Found ~**50% idle GPU time** across the last 3 months.  
   - Initiated predictive autoscaling with a 20% buffer.  

---

## Result  
- Delivered an **average unit cost per generation in USD** that fully covers infrastructure spend.  
- Provided finance and product teams with a solid basis for the new **credit-based pricing**.  
- Identified ~50% idle time and initiated predictive scaling project (under NDA), reducing GPU spend by **30%**.  

---

## Stack  
- **Database:** PostgreSQL  
- **Queries:** SQL (time series, cron parsing, window functions)  
- **Analysis & Visualization:** Jupyter Notebook (Python, Pandas, Matplotlib)  

---