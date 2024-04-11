# DBT UDF 用法

我是 Migo Data Engineer Team 的 Data Engineer Joshua，我們團隊使用 DBT 來進行數據 Transformation，且使用 Bigquery 作為資料倉儲，此篇文章是要介紹如何在 DBT 上管理 UDF。

# 前言

- DBT：數據團隊可以使用 DBT 來進行 ETL 的 Transformation 階段，透過 DBT 可以管理數據表間的關聯、檢測數據等功能
- Bigquery UDF：BQ 上可以創建自定義的函式，叫做 UDF，常用的計算 (例如將時間字串 parse 為 datetime) 可以寫成 UDF 來確保每次轉換數據時用的做法一致且能節省時間

# 在 DBT 上管理 UDF

Bigquery 的 UDF 可以透過以下語法在 BQ 中創建

```sql
CREATE OR REPLACE FUNCTION `Joshua.test_udf`(timestamp_str STRING) RETURNS DATETIME AS(
  PARSE_DATETIME('%Y-%m-%d', timestamp_str)
)
```

## 為何需要在在 DBT 上管理 UDF

如果直接在 BQ 創建 UDF，無法對 UDF 進行版本控制，所以我們一開始是開一個 repo，在上面管理 UDF 的程式碼，當有人新增/修改 UDF 時，部署會同時創建/修改 UDF。但此方法會跟 DBT 脫鉤，在 DBT 如果需要用到 UDF，就需要在切換到另個 repo 去開發 UDF，所以我們團隊才想將 UDF 拉到 DBT 上管理

 

## 官方推薦做法

說明前先簡介一下 macro 的概念，後面會常常用到 macro 

- 根據 [DBT 官方文件](https://docs.getdbt.com/docs/build/jinja-macros) 的介紹， macro 是在 SQL 檔中使用一種叫做 Jinja 的模板語言，Jinja 語法與 python 很相似，可以將  macro 想像成 python 的函數，可以幫助我們生成 SQL 語法。

在 [DBT 官方文件](https://docs.getdbt.com/docs/build/hooks-operations#getting-started-with-hooks-and-operations)上有推薦一篇 [DBT 論壇文章](https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18)來創建 BQ UDF，以下簡介文章做法

1. 創建 UDF macro：macro 裡面寫創建 UDF 語法
    
    ```sql
    # macros/udf/parse_datetime.sql
    {% macro parse_datetime() %}
    CREATE OR REPLACE FUNCTION `joshua-1000.udf.parse_datetime`(timestamp_str STRING) RETURNS DATETIME AS(
      PARSE_DATETIME('%Y-%m-%d', timestamp_str)
    )
    {% endmacro %}
    ```
    
2. 創建 `create_udfs`：裡面會呼叫所有 UDF 相關 macro
    
    ```sql
    # macros/create_udfs.sql
    {% macro create_udfs() %}
      {% do run_query(test_udf()) %};
      {% do run_query(test_udf1()) %};
      {% do run_query(test_udf2()) %};
    {% endmacro %}
    ```
    
3. 執行 dbt command：呼叫 `create_udfs`
    
    ```sql
    dbt run-operation create_udf
    ```
    

但此做法需要一列一列寫 run_query 語法，有 10 個就要寫 10 行，如果 UDF 數量成長到 100 個就得寫 100 行，所以我們團隊決定優化此寫法。

## 優化後寫法

官方推薦文章的解法有以下的缺點：

- 創建 UDF 時要創建 UDF macro 且需要在 `create_udfs` 中新增一列（如果忘記新增 UDF 無法被創建）
- `create_udfs` 中有幾個 UDF 就需要寫多少行
- 每次執行都重新建立所有 UDF（無法只重建有修改的 UDF）
- 無法管理 UDF 上下游關係
- 無法平行創建 UDF
- 使用 UDF 時無法區分環境(production, develop, staging)

因此以下的優化版本希望能解決以上的缺點

### 優化版本一

1. 創建 UDF macro：macro 裡面寫創建 UDF 語法，並用 `run_query` 執行語法
    
    ```sql
    # macros/udf/parse_datetime.sql
    {% macro parse_datetime() %}
      {% set query %}
        CREATE OR REPLACE FUNCTION `joshua-1000.udf.parse_datetime`(timestamp_str STRING) RETURNS DATETIME AS(
          PARSE_DATETIME('%Y-%m-%d', timestamp_str)
        )
      {% endset %}
      {% do run_query(query) %}
    {% endmacro %}
    ```
    
2. 在 models 底下創建 UDF folder，並創建 model：model 內容只是為了呼叫 UDF macro
    
    ```sql
    # models/parse_datetime.sql
    SELECT {{ parse_datetime }} AS UDF
    ```
    

來檢查版本一解決了哪些官方推薦作法的缺點      

- 創建 UDF 時要創建 UDF macro 且需要在 `create_udfs` 中新增一列（如果忘記新增 UDF 無法被創建）→ 部分解決，開發 UDF 時仍需要建立 macro 和 model
- `create_udfs` 中有幾個 UDF 就需要寫多少行 → 解決，透過 model 來創建 UDF
- 每次執行都重新建立所有 UDF（無法只重建有修改的 UDF）→ 解決，每次只 dbt run 修改的 UDF
- 無法管理 UDF 上下游關係 → 解決，透過 model 來建立連結
- 無法平行創建 UDF → 解決，model 可以平行創建
- 使用 UDF 時無法區分環境(production, develop, staging) → 無解，使用 UDF 時仍只能寫死（`joshua-1000.udf.parse_datetime`）

雖然幾乎解決所有缺點，但也衍生更多問題，像是開發 UDF 時需要建立 macro ＋ model、model 的程式碼沒有意義，很快我們就朝向其他方向優化，產生第二個版本

### 優化版本二

1. 創建 `ddl_generator` macro ：省去每次開發 UDF 時都要重複寫開頭語法
    
    ```sql
    #macros/ddl_generator.sql
    {% macro ddl_generator(function_name, type, params, reture_type) %}
    	{%- set params_string -%}
        {%- for param in params -%}
          {{ param }}
          {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      {%- endset -%}
      
      {% set return_string = '' %}
      {% if type == 'FUNCTION' %}
    	  {% set return_string = 'RETURNS' + return_type %}
    	{% endif %}
    	CREATE OR REPLACE {{ type }} `project.udf.{{ function_name }}` ({{ params_string }}) {{ return_string }} AS 
    {% endmacro %}
    ```
    
2. 創建 UDF macro：macro 裡面寫創建 UDF 語法
    
    ```sql
    #macros/udf/parse_datetime.sql
    {% macro parse_datetime() %}
    	{{ 
    		ddl_generator(
    			function_name = 'test_udf',
    			type = 'FUNCTION',
    			params = [
    				'timestamp_str STRING'
    			],
    			return_type = 'DATETIME'
    		) 
    	}}
    	(
    		PARSE_DATETIME('%Y-%m-%d', timestamp_str)
    	)
    {% endmacro %}
    ```
    
3. 創建 `create_udfs`：裡面會呼叫參數傳入的 UDF macro
    
    ```sql
    #macros/create_udfs.sql
    {% macro create_udfs(function_list=[], table_function_list=[]) %}
    	{% for function in function_list %}
    			{% set macro_func = context.get(function)%}
    			{% do run_query(macro_func()) %};
    	{% endfor %}
    	{% for table_function in table_function_list %}
    			{% set macro_func = context.get(table_function)%}
    			{% do run_query(macro_func()) %};
    	{% endfor %}
    {% endmacro %}
    ```
    
4. 改 Jenkins file：當 macros folder 下 UDF macro 有改動時，會捕捉有更動的 macro 名稱，並存成 list 作為參數傳到 `create_udfs` 中
    
    ```bash
    dbt run-operation create_udfs --args '{function_list = ['parse_datetime']}'
    ```
    
5. 使用 UDF：透過 `udf` macro 來 call UDF
    
    ```sql
    #macros/udf.sql
    {% macro udf(function_name) %}
      {% set dataset = 'udf' %}
      {% if var('env', none) == 'ci' %}
        {% set dataset = 'udf_ci' %}
      {% elif var('env', none) == 'staging' %}
        {% set dataset = 'udf_staging' %}
      {% elif var('env', none) == 'develop' %}
        {% set dataset = 'udf_dev' %}
      {% endif %}
      `joshua-1000.{{dataset}}.{{function_name}}`
    {% endmacro %}
    ```
    

來檢查版本二解決了哪些官方推薦作法的缺點      

- 創建 UDF 時要創建 UDF macro 且需要在 `create_udfs` 中新增一列（如果忘記新增 UDF 無法被創建）→ 解決，只需要建立 macro
- `create_udfs` 中有幾個 UDF 就需要寫多少行 → 解決，在 Jenkins file 會補捉改動的 UDF macro，再將修改的 macro 名稱傳遞給 `create_udfs`
- 每次執行都重新建立所有 UDF（無法只重建有修改的 UDF）→ 解決，Jenkins file 只會補捉改動的 UDF macro
- 無法管理 UDF 上下游關係 → 無解，`create_udfs` 只會迴圈建立改動的 UDF macro，無法決定創建順序
- 無法平行創建 UDF → 無解，`create_udfs` 為迴圈建立
- 使用 UDF 時無法區分環境(production, develop, staging) → 解決，使用 `udf` macro 可以根據不同 env 參數來改變不同環境的 dataset

版本二放棄使用 model 來開發 UDF，所以無法達到並行創建、管理上下游關係的問題，但開發上較為直觀，也解決了官方推薦做法許多問題（每次執行都重新建立所有 UDF、需要多行創建 UDF、區分環境  ），也透過 `ddl_generator` macro 來省去撰寫 UDF 開頭語法的時間  

在權衡之下，我們團隊一開始是選擇使用版本二，但在改寫途中意外發現 DBT 的 `materialization` ，才產生最終的解法

## UDF materialization

[DBT materialization](https://docs.getdbt.com/docs/build/materializations) 是將 models 實現在資料倉儲中的策略，官方預設有 5 種 `materialization`，常見的有 `table, view, incremental`，在 `dbt_project.yml` 或是 model config 中可以對 model 設定其 materialized（例如設定為 `view` ，該 model 在資料倉儲就會被建立為 `view`）

除了使用官方預設的 `materialization`，也可以自行創建 `materialization`，官方也有出[教學介紹](https://docs.getdbt.com/guides/create-new-materializations?step=1)

使用 `materialization` 來建立 UDF 可以解決上述兩個的短板，並擁有兩者的優點。以下就介紹我們團隊如何實作 UDF `materialization`

### 建立 UDF materialization

UDF 有兩種類型：`function` 和 `table function`，在建立語法上有稍許不同，所以我們建立兩個 `materialization`，以下用 `function` 作為例子

1. 在 macros folder 下建立 `function materialization`
    
    ```sql
    # macros/udf/function.sql
    {% materialization function, adapter='bigquery' %}
      -- 檢查 relation 是否已存在（如果存在且為其他類型報錯）
      {% set target_relation = this %}
      {% set existing_relation = load_cached_relation(this) %}
      {% if existing_relation is not none %}
        {{ exceptions.raise_compiler_error('Relation "' ~ target_relation ~ '" exists as ' ~ existing_relation.type) }}
      {% endif %}
      
      -- 執行 pre-hooks
      {{ run_hooks(pre_hooks) }}
      
      -- 取得 model description（附錄會有詳細說明 ）
      {% set model_description =  model.description %}
     
      -- 執行建立 UDF 語法
      {% call statement('main') -%}
        {{ get_create_function_as_sql(target_relation, sql, config, model_description) }}
      {%- endcall %}
     
      {{ adapter.commit() }}
     
      -- 執行 post-hooks
      {{ run_hooks(post_hooks) }}
      
      -- 更新 Relation cache：幫助 DBT 快速確認此 relation 存在
      {{ return({'relations': [target_relation]}) }}
     
    {% endmaterialization %}
    ```
    
2. 建立 `get_create_function_as_sql` macro：產生建立 UDF 語法
    
    ```sql
    # macros/udf/get_create_function_as_sql.sql
    {% macro get_create_function_as_sql(relation, sql, config, model_description) -%} 
    
      {%- set return_type = config.require('return_type') -%}
    
      {%- set params = config.require('params') -%}
      {%- set params_string -%}
        {%- for param in params -%}
          {{ param }}
          {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      {%- endset -%}
    
      CREATE OR REPLACE FUNCTION {{ relation }} ({{ params_string }}) RETURNS {{ return_type }} 
      OPTIONS(description="""{{ model_description }}""") # 將寫在 schema.yml 的 description 寫入到 BQ
      AS (
        {{ sql }}
      );
    {%- endmacro %}
    
    ```
    
3. `dbt_project.yml` 設定 udf dataset `materialization`
    
    ```yaml
    models:
      udf:
        +tags: udf
        +database: joshua-1000
        +schema: udf
        function:
          +materialized: function
        table_function:
          +materialized: table_function
    ```
    
4. 在 models 底下建立 function folders，並建立 UDF models
    
    ```sql
    # models/udf/function/parse_datetime.sql
    
    {{ 
      config(
        params = [
          'timestamp_str STRING'
        ],
        return_type = 'DATETIME'
      ) 
    }}
    
    PARSE_DATETIME('%Y-%m-%d', timestamp_str)
    
    ```
    

透過 materialization 建立 UDF relation 後，跟建立 table/view 一樣可以透過 ref 來建立 dependency

```sql
# models/test_table.sql

SELECT 
  {{ ref('test_udf') }}('2023-11-01') AS datetime
```

也能透過 dbt run 來建立 UDF

```bash
# 這裡使用 Migo 實際在用的 UDF
dbt run --select udf.function.lazy_parse_datetime --vars '{env: staging}'
```

![截圖 2024-03-25 下午6.01.27.png](DBT%20UDF%20%E7%94%A8%E6%B3%95%20764ad64940e84639900ed65ba2a6f428/%25E6%2588%25AA%25E5%259C%2596_2024-03-25_%25E4%25B8%258B%25E5%258D%25886.01.27.png)

來檢查最終版本解決了哪些官方推薦做法的缺點      

- 創建 UDF 時要創建 UDF macro 且需要在 `create_udfs` 中新增一列（如果忘記新增 UDF 無法被創建）→ 解決，只需要建立 model
- `create_udfs` 中有幾個 UDF 就需要寫多少行 → 解決，也不需要改動 Jenkins file
- 每次執行都重新建立所有 UDF（無法只重建有修改的 UDF）→ 解決，Jenkins file 只會補捉改動的 UDF macro
- 無法管理 UDF 上下游關係 → 解決，model 可以透過 `ref` 來建立上下游關係
- 無法平行創建 UDF → 解決，model 可以平行創建
- 使用 UDF 時無法區分環境(production, develop, staging) → 解決，且不需要額外的 macro 就能達成

UDF materialize 後就能使用 model 的特性，且在開發上直觀方便，解決所有原本作法的缺點！

以上就是如何在 DBT 上管理 UDF 的做法，如果有問題、想法或是覺得可以優化的地方歡迎留言或來信交流意見！

## 附錄

在 UDF `materialization` 中有一段是處理 model description，這段是將我們寫在 `schema.yml` 上的 description 寫到 BQ  udf 說明  

![截圖 2024-04-10 下午5.29.36.png](DBT%20UDF%20%E7%94%A8%E6%B3%95%20764ad64940e84639900ed65ba2a6f428/%25E6%2588%25AA%25E5%259C%2596_2024-04-10_%25E4%25B8%258B%25E5%258D%25885.29.36.png)

### DBT 內建做法

DBT 有內建一個 [persist_docs macro](https://docs.getdbt.com/reference/resource-configs/persist_docs)，只要在 `dbt_project.yml` 加上 `+persist_docs` 就能將寫在 `schema.yml` 中 model 的 description 寫入到 BQ 的說明中

```sql
# dbt_project.yml
models:
  +persist_docs:
    relation: true
    columns: true
```

但這不適用於我們剛建立的 UDF `materialization`，因為 `persist_docs` 背後是透過 `ALTER` 語法來把 description 寫到 BQ，但在 BQ 上 `function` 和 `table_function` 無法使用 `ALTER` 語法，所以我們打算在 `materialization` 的過程實作寫入說明的部分

### UDF Materialization 寫入說明

首先要先取得 `schema.yml` 中的 description，在建立 `function` 和 `table_function` 時，多寫 `OPTION` 這段將 description 寫入，就能在 BQ 中看到 UDF 的說明了   

底下的 code 是擷取 `function materialization` 和 `get_create_function_as_sql` macro 的程式碼

```sql
# udf/function.sql
{% set model_description =  model.description %}

# udf/get_create_function_as_sql.sql
CREATE OR REPLACE FUNCTION {{ relation }} ({{ params_string }}) RETURNS {{ return_type }} 
  OPTIONS(description="""{{ model_description }}""") # 將寫在 schema.yml 的 description 寫入到 BQ
  AS (
    {{ sql }}
  );
```

## 參考資料

- [https://docs.getdbt.com/docs/build/jinja-macros](https://docs.getdbt.com/docs/build/jinja-macros)
- [https://docs.getdbt.com/docs/build/hooks-operations#getting-started-with-hooks-and-operations](https://docs.getdbt.com/docs/build/hooks-operations#getting-started-with-hooks-and-operations)
- [https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18](https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18)
- [https://docs.getdbt.com/docs/build/materializations](https://docs.getdbt.com/docs/build/materializations)
- [https://docs.getdbt.com/reference/resource-configs/persist_docs](https://docs.getdbt.com/reference/resource-configs/persist_docs)