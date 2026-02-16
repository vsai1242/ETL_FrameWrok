# 📊 CSV-DRIVEN ETL VALIDATION GUIDE

## 🎯 For Manual Testers - No Coding Required!

### **What You Can Do:**
1. Define source and target SQL queries in CSV
2. Choose validation type (row count, schema, null check, etc.)
3. Use variables like `{recid_list}` for dynamic queries
4. Enable/disable tests without touching code
5. Get beautiful Allure reports automatically

---

## 📋 CSV File Structure

**File:** `data/etl_validation_tests.csv`

### **Columns:**

| Column | Description | Example |
|--------|-------------|---------|
| **test_id** | Unique test identifier | TEST_01, TEST_02 |
| **test_name** | Descriptive test name | Total Record Count |
| **source_query** | SQL query for source (Bronze) | SELECT COUNT(*) FROM Bronze.table |
| **target_query** | SQL query for target (Silver) | SELECT COUNT(*) FROM Silver.table |
| **validation_type** | Type of validation to perform | row_count_comparison |
| **enabled** | TRUE/FALSE to run test | TRUE |
| **description** | What the test validates | Validate counts match |
| **severity** | critical/high/normal/low | critical |

---

## 🔧 Validation Types Available

### **1. row_count_comparison**
Compares row counts between source and target
```csv
TEST_01,Total Count,SELECT COUNT(*) FROM source,SELECT COUNT(*) FROM target,row_count_comparison,TRUE,Count validation,critical
```

### **2. schema_and_datatype_validation**
Validates schema and data types match
```csv
TEST_05,Schema Check,SELECT * FROM source WHERE 1=0,SELECT * FROM target WHERE 1=0,schema_and_datatype_validation,TRUE,Schema validation,normal
```

### **3. null_checks_mandatory_columns**
Checks for NULL values in mandatory columns
```csv
TEST_06,Null Check,SELECT * FROM source,SELECT * FROM target,null_checks_mandatory_columns,TRUE,Check nulls,high
```

### **4. duplicate_checks_primary_keys**
Validates primary keys exist in target (uses recid)
```csv
TEST_04,RecID Check,SELECT recid FROM source,SELECT recid FROM target WHERE recid IN ({recid_list}),duplicate_checks_primary_keys,TRUE,Validate recids,normal
```

### **5. aggregate_validations**
Compares aggregate values (SUM, AVG, etc.)
```csv
TEST_07,Sum Check,SELECT SUM(amount) FROM source,SELECT SUM(amount) FROM target,aggregate_validations,TRUE,Sum validation,critical
```

---

## 🎯 Using Variables in Queries

### **Problem:** You want to get recids from Bronze and check if they exist in Silver

### **Solution:** Use `{recid_list}` variable!

**Step 1:** Source query gets the recids
```sql
SELECT TOP 10 recid FROM LH_AX_CANADA.fullload.CUSTINVOICEJOUR 
WHERE dpcreateddatetime=dpmodifieddatetime 
ORDER BY dpmodifieddatetime DESC
```

**Step 2:** Target query uses `{recid_list}` variable
```sql
SELECT recid FROM LH_Finance.dbo.CUSTINVOICEJOUR 
WHERE recid IN ({recid_list}) AND lhname='LH_AX_CANADA'
```

**What Happens:**
1. Framework executes source query
2. Extracts recids: `[123, 456, 789]`
3. Replaces `{recid_list}` with `123,456,789`
4. Executes target query with actual recids
5. Validates all recids found

---

## 📝 Example CSV Entries

### **Example 1: Simple Count Comparison**
```csv
TEST_01,Total Record Count,SELECT COUNT(*) FROM LH_AX_CANADA.fullload.CUSTINVOICEJOUR,SELECT COUNT(*) FROM LH_Finance.dbo.CUSTINVOICEJOUR WHERE lhname='LH_AX_CANADA',row_count_comparison,TRUE,Validate total record count matches,critical
```

### **Example 2: Deleted Records**
```csv
TEST_02,Deleted Records Count,SELECT COUNT(*) FROM LH_AX_CANADA.fullload.CUSTINVOICEJOUR WHERE isdelete=1,SELECT COUNT(*) FROM LH_Finance.dbo.CUSTINVOICEJOUR WHERE isdelete=1 AND lhname='LH_AX_CANADA',row_count_comparison,TRUE,Validate deleted records match,high
```

### **Example 3: RecID Validation with Variable**
```csv
TEST_04,Inserted Records with RecID,SELECT TOP 10 recid FROM LH_AX_CANADA.fullload.CUSTINVOICEJOUR WHERE dpcreateddatetime=dpmodifieddatetime ORDER BY dpmodifieddatetime DESC,SELECT recid FROM LH_Finance.dbo.CUSTINVOICEJOUR WHERE recid IN ({recid_list}) AND lhname='LH_AX_CANADA',duplicate_checks_primary_keys,TRUE,Validate inserted records using recid,normal
```

### **Example 4: Multiple Tables with UNION**
```csv
TEST_08,Multi-Table RecID,SELECT recid FROM (SELECT TOP 5 recid FROM table1 UNION ALL SELECT TOP 5 recid FROM table2) AS combined,SELECT recid FROM target WHERE recid IN ({recid_list}),duplicate_checks_primary_keys,TRUE,Multi-table validation,high
```

---

## 🚀 How to Run Tests

### **Run All CSV Tests:**
```bash
python -m pytest tests/fabric/test_csv_driven_etl_validation.py -v --alluredir=reports/allure-results
```

### **Run Specific Test:**
```bash
python -m pytest tests/fabric/test_csv_driven_etl_validation.py::TestBronzeToSilverCSV::test_01_total_record_count_csv -v
```

### **Generate Allure Report:**
```bash
cd allure-2.32.0\bin
.\allure.bat serve ..\..\reports\allure-results
```

---

## 📊 What You Get in Allure Report

For each test, you'll see:
- ✅ **Test Configuration** - What queries were used
- ✅ **Source Query Results** - Sample data from Bronze
- ✅ **Target Query Results** - Sample data from Silver
- ✅ **Validation Results** - Pass/Fail with details
- ✅ **RecID Lists** - When using variables
- ✅ **Error Messages** - Clear failure reasons

---

## 🎯 Quick Start for Manual Testers

### **Step 1: Open CSV File**
```bash
data/etl_validation_tests.csv
```

### **Step 2: Add Your Test**
```csv
TEST_NEW,My Test Name,<your source query>,<your target query>,row_count_comparison,TRUE,My description,normal
```

### **Step 3: Run Tests**
```bash
python -m pytest tests/fabric/test_csv_driven_etl_validation.py -v --alluredir=reports/allure-results
```

### **Step 4: View Report**
```bash
cd allure-2.32.0\bin
.\allure.bat serve ..\..\reports\allure-results
```

---

## 🔥 Advanced Examples

### **Example: Complex RecID Query with Multiple Tables**
```csv
TEST_COMPLEX,Multi-Table Insert,"
SELECT recid FROM (
    SELECT TOP 10 recid FROM LH_AX_CANADA.fullload.CUSTINVOICEJOUR 
    WHERE dpcreateddatetime=dpmodifieddatetime
    UNION ALL
    SELECT TOP 10 recid FROM LH_AX_CANADA.fullload.CUSTTABLE
    WHERE dpcreateddatetime=dpmodifieddatetime
) AS combined
ORDER BY recid DESC
","
SELECT recid FROM (
    SELECT recid FROM LH_Finance.dbo.CUSTINVOICEJOUR WHERE recid IN ({recid_list})
    UNION ALL
    SELECT recid FROM LH_Finance.dbo.CUSTTABLE WHERE recid IN ({recid_list})
) AS combined
WHERE lhname='LH_AX_CANADA'
",duplicate_checks_primary_keys,TRUE,Complex multi-table recid validation,high
```

---

## ✅ Benefits

1. **No Coding Required** - Just SQL queries in CSV
2. **Dynamic Variables** - Use `{recid_list}` for complex scenarios
3. **Reusable Validations** - Predefined validation methods
4. **Beautiful Reports** - Allure with all details
5. **Easy Maintenance** - Update CSV, not code
6. **Version Control** - Track test changes in CSV

**Result: Manual testers can create comprehensive ETL validations without writing any Python code!** 🎯