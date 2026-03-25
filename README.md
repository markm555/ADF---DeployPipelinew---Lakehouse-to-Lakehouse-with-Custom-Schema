# Azure Data Factory → Microsoft Fabric Lakehouse (Custom Schema)
**C# / ARM REST–based deployment and execution**

MIT License
Copyright (c) 2026 Mark Moore
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


## Overview

This project demonstrates how to **programmatically deploy and execute an Azure Data Factory (ADF) pipeline** using **pure ARM REST APIs** and **C#**, copying data from an **on‑premises SQL Server** into a **Microsoft Fabric Lakehouse table** while **explicitly targeting a custom schema**.

> ⚠️ **Important – Temporary Workaround**
>
> This code exists to address customer scenarios where **schema and table must be supplied as separate properties** when writing to a **schema‑enabled Fabric Lakehouse**, prior to full first‑class support being available across all deployment paths.
>
> Once the corresponding Fabric / ADF enhancements are fully implemented by the Product Group, portions of this code may be simplified or removed.

---

## What This Program Does

The program performs the following end‑to‑end steps:

1. **Authenticates to Azure Resource Manager (ARM)**
   - Uses `DefaultAzureCredential`
   - Supports:
     - Local development via `az login` or Visual Studio sign‑in
     - Managed Identity when deployed

2. **Creates or Updates ADF Linked Services**
   - **On‑premises SQL Server** using a **Self‑Hosted Integration Runtime (SHIR)**
   - **Microsoft Fabric Lakehouse** using the native **Lakehouse connector**

3. **Creates or Updates ADF Datasets**
   - SQL Server table (source)
   - Fabric Lakehouse table (sink), with:
     - `schema` and `table` defined **as separate properties**

4. **Creates or Updates an ADF Pipeline**
   - Single **Copy activity**
   - SQL Server → Fabric Lakehouse

All operations are performed using **ARM REST APIs (`2018-06-01`)** — no portal interaction and no ADF SDK abstractions.

## Architecture

This solution uses **Azure Data Factory (ADF)** as the orchestration layer to move data from an **on‑premises SQL Server** into a **Microsoft Fabric Lakehouse**, explicitly targeting a **non‑default schema**.

The architecture is intentionally **code‑first** and **REST‑driven**, avoiding portal configuration and SDK abstractions.

---

### High‑Level Flow
---

### Components

#### On‑Premises SQL Server
- Acts as the **source system**
- Accessed via standard SQL authentication
- Connectivity to Azure is enabled through a **Self‑Hosted Integration Runtime (SHIR)**

---

#### Self‑Hosted Integration Runtime (SHIR)
- Provides a **secure bridge** between Azure Data Factory and on‑premises resources
- Installed inside the customer network
- Referenced by name in the SQL Server linked service
- No inbound firewall rules required

---

#### Azure Data Factory
ADF serves as the **control plane and orchestration engine**:

- Linked Services define connectivity:
  - SQL Server (via SHIR)
  - Fabric Lakehouse (via native Lakehouse connector)
- Datasets define source and sink metadata
- A Pipeline coordinates execution using a **Copy activity**
- All resources are created or updated via **ARM REST APIs**

---

#### Microsoft Fabric Lakehouse
- Acts as the **destination system**
- Uses a **schema‑enabled Lakehouse**
- Target table is defined using:
  - `typeProperties.schema`
  - `typeProperties.table`
- This avoids legacy patterns such as embedding schema names inside the table identifier

---

#### Service Principal Authentication
- A Service Principal is used to authenticate ADF to Fabric
- Credentials are supplied to the Lakehouse linked service
- Permissions are scoped to:
  - Fabric workspace
  - Lakehouse write access

> ⚠️ Secrets are shown inline only for demonstration.
> Production deployments should retrieve secrets from Azure Key Vault.  See https://github.com/markm555/KeyVaultUsageDemo for sample functions to write and read secrets to keyvault.

---

### Why REST Instead of SDKs or Portal

This architecture intentionally uses **raw ARM REST calls** to:

- Enable **fully automated deployments**
- Support **code‑first organizations**
- Avoid SDK lag during Fabric feature rollouts
- Provide deterministic behavior across environments

All ADF artifacts (linked services, datasets, pipelines) are deployed using:
- `PUT` (create or update)
- `If-Match: *` for idempotent updates

---

### Design Characteristics

- ✅ Fully automated (no UI dependency)
- ✅ Environment‑agnostic
- ✅ Schema‑aware Fabric writes
- ✅ Compatible with CI/CD pipelines
- ✅ Minimal Azure permissions required

---

### Known Constraints

- This architecture exists as a **temporary workaround**
- Future Fabric / ADF enhancements may eliminate the need for:
  - Explicit schema handling
  - REST‑level dataset construction
