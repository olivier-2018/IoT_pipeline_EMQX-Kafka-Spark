# EMQX configuration

## 📚 Official documentation reference

EMQX docs on configuration structure:  
**`https://docs.emqx.com/en/emqx/latest/configuration/configuration.html` [(docs.emqx.com in Bing)](https://www.bing.com/search?q="https%3A%2F%2Fdocs.emqx.com%2Fen%2Femqx%2Flatest%2Fconfiguration%2Fconfiguration.html")**

EMQX docs on rule engine:  
**`https://docs.emqx.com/en/emqx/latest/data-integration/rules.html` [(docs.emqx.com in Bing)](https://www.bing.com/search?q="https%3A%2F%2Fdocs.emqx.com%2Fen%2Femqx%2Flatest%2Fdata-integration%2Frules.html")**

## ⚙️ Static vs Dynamic Config (EMQX 6.x)

EMQX writes JSON/HOCON fragments here so that **your rules persist across container restarts**.

| Location | Purpose |
|---------|---------|
| `/opt/emqx/etc` | **Static config** (loaded at startup) |
| `/opt/emqx/data/configs` | **Dynamic config** (created by dashboard/API) |

**Static configuration files**

- `emqx.conf`
- `base.hocon`
- `cluster.hocon`
- `plugins/*.hocon`

**dynamic configuration**

- `rule_engine.conf`
- `data_bridge.conf`
- `connectors.conf`
- `actions.conf`

dynamic configuration include:  
- Anything you create in the **Dashboard**  
- Anything you create via the **REST API**  
- Rules, actions, connectors, bridges, etc.

---



## 🧩 How EMQX loads configuration (static vs dynamic)

### **Static config**  
Loaded at startup from:

- `/opt/emqx/etc/emqx.conf`
- `/opt/emqx/etc/base.hocon`
- `/opt/emqx/etc/cluster.hocon`
- `/opt/emqx/etc/*.hocon`
- `/opt/emqx/etc/plugins/*.hocon`

Static config is ideal for:

- Kafka connectors  
- Rules you want to version‑control  
- Infrastructure‑as‑code setups  

### **Dynamic config**  
Stored in:

- `/opt/emqx/data/configs`
- `rule_engine.conf`
- `data_bridge.conf`
- `connectors.conf`
- `actions.conf`

Dynamic config is created when you:

- Use the **Dashboard**  
- Use the **REST API**  

Dynamic config is ideal for:

- Quick prototyping  
- Editing rules interactively  
- Persisting dashboard‑created rules across container restarts  

If you mount `/opt/emqx/data/configs`, dashboard‑created rules persist.

---

## 🧠 Should you put rules in `/opt/emqx/data/configs`?

Only if you want:

- Dashboard‑managed rules  
- No manual editing  
- Persistence across restarts  

But **you should NOT manually edit files inside `/opt/emqx/data/configs`**.  
EMQX writes and manages them internally.

So the answer is:

> **Do NOT manually write rules into `/opt/emqx/data/configs`.  
> Use static config (`emqx.conf` / `base.hocon`) OR the REST API.**

