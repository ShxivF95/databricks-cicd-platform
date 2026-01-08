import dlt
from pyspark.sql.functions import expr

# ===================== gold_dim_patients =====================

@dlt.table(
    name="gold_dim_patients",
    comment="Current patients dimension for BI"
)
def gold_dim_patients():
    return (
        dlt.read_stream("sil_patientsdata")
          .filter("__END_AT IS NULL")
          .select(
              "Patient_ID",
              "Patient_Name",
              "GENDER",
              "DOB",
              "ZIPCODE",
              "Mobile_no"
          )
    )

# ===================== gold_routinetests =====================

@dlt.table(
    name="gold_routinetests",
    comment="Gold routine tests with reporting SLA metrics"
)
def gold_routinetests():
    return (
        dlt.read_stream("sil_routinetests")
          .withColumn(
              "report_delay_minutes",
              expr(
                  "timestampdiff(MINUTE, Test_Done_datetime, Report_released_datetime)"
              )
          )
          .withColumn(
              "report_delay_hours",
              expr(
                  "timestampdiff(SECOND, Test_Done_datetime, Report_released_datetime) / 3600.0"
              )
          )
    )
