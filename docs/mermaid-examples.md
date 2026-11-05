# Mermaid diagram examples

[Mermaid](https://mermaid.js.org/) turns short text into diagrams. GitHub renders any ```` ```mermaid ```` block
in a Markdown file, so these show as pictures on GitHub. Copy one, change the names, and it updates.
Tip: [mermaid.live](https://mermaid.live) previews as you type.

Examples go from simple to more involved, using the jobs in this repo.

---

## 1. A simple flow

`LR` = left to right (`TD` = top down). `-->` is an arrow.

```mermaid
flowchart LR
    A[Check inputs] --> B[Clean] --> C[Summarize]
```

## 2. Node shapes

The brackets around a label set its shape.

```mermaid
flowchart LR
    start([Start]) --> file[/Batch file/]
    file --> step[Clean data]
    step --> ok{Valid?}
    ok -- yes --> db[(Database)]
    ok -- no --> stop((Stop))
    db --> sub[[Shared script]]
```

| Shape | Syntax | Typical use |
|---|---|---|
| Rectangle | `id[Text]` | a step / task |
| Rounded pill | `id([Text])` | start or end |
| Parallelogram | `id[/Text/]` | a file or input |
| Diamond | `id{Text}` | a decision |
| Cylinder | `id[(Text)]` | a database |
| Circle | `id((Text))` | a stop or event |
| Double-sided box | `id[[Text]]` | a shared routine |

## 3. Arrow styles and labels

```mermaid
flowchart LR
    a[Task A] -->|normal| b[Task B]
    b -.->|optional| c[Task C]
    a ==>|important| d[Task D]
    b --x e[Not allowed]
```

`-->` solid, `-.->` dotted, `==>` thick, `--x` ends in a cross, `-->|label|` adds text.

## 4. An Airflow job as a DAG (example 3)

```mermaid
flowchart LR
    validate --> filter_data --> summarize --> done
```

## 5. Fan-out, fan-in, and "no loops"

Parallel steps branch out and join again. A dashed red arrow back to the start shows a loop, which Airflow rejects.

```mermaid
flowchart LR
    validate --> pick_regions
    pick_regions --> e[filter East] --> se[summarize East] --> combine
    pick_regions --> w[filter West] --> sw[summarize West] --> combine
    pick_regions --> n[filter North] --> sn[summarize North] --> combine
    n -. "loop: not allowed" .-x validate
    linkStyle 10 stroke:#B3261E,stroke-width:2px
```

## 6. Colouring by state, with a task group

`classDef` defines a style; `class` applies it. `subgraph` draws a box around related steps.

```mermaid
flowchart LR
    v[validate]:::ok --> g
    subgraph g [extract · task group]
        e1[extract East]:::ok
        e2[extract West]:::failed
    end
    g --> r[build_report]:::upstream
    r --> f[start follow-up job]:::skipped

    classDef ok fill:#E3F1E8,stroke:#2E7D4F
    classDef failed fill:#F8E1DF,stroke:#B3261E
    classDef upstream fill:#FCEBD3,stroke:#B26A00
    classDef skipped fill:#EFEDE8,stroke:#A9A59A,stroke-dasharray:4 4
```

## 7. Branching on an input (example 4)

```mermaid
flowchart LR
    load_rows --> choose{report_type?}
    choose -- summary --> make_summary
    choose -- detail --> make_detail
    choose -- dry run --> dry_run_preview
    make_summary --> finish
    make_detail --> finish
    dry_run_preview --> finish
```

## 8. A batch job with files, scripts and a database (example 8)

Every step runs through the same shared `run_python_step.sh`; each box shows which script it runs.

```mermaid
flowchart TD
    batch[/"Batch folder: inventory_counts.csv"/] --> check[check_inputs]
    check --> clean["clean_counts<br/><i>team/inventory/</i>"]
    clean --> cleaned[/counts_clean.csv/]
    cleaned --> summarize["summarize_counts<br/><i>shared/reporting/</i>"]
    summarize --> report[/counts_by_store.csv/]
    report --> ask{Insert ticked?}
    ask -- yes --> load["load_counts<br/><i>team/inventory/</i>"]
    ask -. no .-> skip([load skipped])
    load --> db[(inventory_counts)]
```

## 9. Two tasks sharing data through the database (example 9)

Task 2 never receives task 1's DataFrame; it reads task 1's rows back from the database.

```mermaid
flowchart TD
    subgraph t1 ["Task 1 · load_trips"]
        direction LR
        a1[/trips.csv/] --> a2[build_trips_df] --> a3[load_df_to_table]
    end
    a3 -->|inserts trips| db[("MySQL: trips, trip_points")]
    db -->|"this batch's trips"| b0
    subgraph t2 ["Task 2 · load_trip_points"]
        direction LR
        b0[read_trips_from_db] --> b2[build_trip_points_df] --> b3[load_df_to_table]
        b1[/trip_points.csv/] --> b2
    end
    b3 -->|inserts points| db2[("MySQL: trip_points")]
```

## 10. ERD: the tables behind examples 8 and 9

`||--o{` reads "one to zero-or-more". `PK` / `FK` mark keys.

```mermaid
erDiagram
    trips ||--o{ trip_points : "has"
    trips {
        string batch_id PK
        string trip_id PK
        datetime start_time
        string depot
        string vehicle_type
        decimal heading_deg
        decimal planned_km
    }
    trip_points {
        string batch_id PK
        string point_id PK
        string trip_id FK
        int t_min
        decimal x_km
        decimal y_km
        decimal elevation_m
        decimal distance_from_depot_km
    }
    inventory_counts {
        string batch_id PK
        string store PK
        string sku PK
        datetime counted_at
        int quantity
    }
```

| Symbol | Meaning |
|---|---|
| `\|\|` | exactly one |
| `o\|` | zero or one |
| `}o` / `o{` | zero or more |
| `}\|` / `\|{` | one or more |

## 11. Sequence: what happens when you click Trigger

```mermaid
sequenceDiagram
    actor You
    participant UI as Web UI
    participant DB as Metadata DB
    participant S as Scheduler
    participant W as Worker

    You->>UI: Fill in form, click Trigger
    UI->>UI: Check inputs against params
    UI->>DB: Save run + inputs
    S->>DB: Any new runs?
    DB-->>S: Yes, this one
    loop each task, in order
        S->>W: Run task
        W->>W: Run script with inputs
        W-->>DB: Status + logs
    end
    UI-->>You: Squares turn green (or red)
```

## 12. State diagram: a task's life

```mermaid
stateDiagram-v2
    [*] --> scheduled
    scheduled --> queued
    queued --> running
    running --> success
    running --> failed
    running --> up_for_retry: error, retries left
    up_for_retry --> scheduled
    scheduled --> upstream_failed: a task before it failed
    scheduled --> skipped: branch not taken
    success --> [*]
    failed --> [*]
```

## 13. Gantt: planning the rollout

```mermaid
gantt
    title Airflow rollout
    dateFormat YYYY-MM-DD
    section Setup
    Install on server        :done,   s1, 2026-11-02, 3d
    Accounts and roles       :active, s2, after s1, 2d
    section Jobs
    Move first batch job     :        j1, after s2, 5d
    Teach-me session         :milestone, m1, 2026-11-18, 0d
```

---

**Reference:** [Mermaid docs](https://mermaid.js.org/intro/) ·
[Flowchart syntax](https://mermaid.js.org/syntax/flowchart.html) ·
[ERD syntax](https://mermaid.js.org/syntax/entityRelationshipDiagram.html) ·
[GitHub: creating diagrams](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/creating-diagrams)
