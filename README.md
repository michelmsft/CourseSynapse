# CourseSynapse

**CourseSynapse** is a practical project built to showcase the capabilities of **Azure Synapse Analytics**. It covers data ingestion, transformation, and analytics using a unified Synapse workspace.

## 📁 Data Lake Folder Structure

All data for this project should be stored in an **Azure Data Lake Storage Gen2** container named `data`. The container is organized by U.S. state folders as follows:

<pre><code>
  data/ 
  ├── TX/ 
  ├── GA/ 
  └── NY/
 </code></pre>

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/michelmsft/CourseSynapse.git
cd CourseSynapse
```
### 2. Set Up Azure Resources
Azure Synapse Workspace – Create one via the Azure portal.
Azure Data Lake Storage Gen2 – Enable hierarchical namespace.
Create a container named data with subfolders:

```bash
TX/ (for Texas data)
GA/ (for Georgia data)
NY/ (for New York data)
```

### 3. Upload Sample Datasets
Place relevant CSV or Parquet files in each state folder.

```bash
data/TX/customers.csv
data/GA/orders.parquet
data/NY/transactions.csv
```
