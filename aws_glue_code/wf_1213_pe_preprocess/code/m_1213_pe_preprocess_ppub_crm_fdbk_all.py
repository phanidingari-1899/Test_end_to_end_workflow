import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "GLUE_DATABASE", "S3_OUTPUT_BUCKET", "SNOWFLAKE_SECRET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Resolved once here so every mapping script stops inventing its own differently-spelled
# placeholder for the same handful of values (six spellings of "the Glue database" alone,
# across scripts, before this). Node-level code must reference these directly, never
# redeclare them.
ENV = args["ENV"]
GLUE_DATABASE = args["GLUE_DATABASE"]
S3_OUTPUT_BUCKET = args["S3_OUTPUT_BUCKET"]

SNOWFLAKE_SECRET_NAME = args["SNOWFLAKE_SECRET_NAME"]
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)


# placeholder for Informatica session file target directory
PM_TARGET_FILE_DIR = "REPLACE_WITH_PM_TARGET_FILE_DIR"

import os
import glob
import shutil
from pyspark.sql.functions import col, trim, when, lit, expr, split, element_at, row_number
from pyspark.sql.window import Window

# --- Source: File_PPUB_CRM_FDBK_ALL -------------------------------------------------
try:
    logger.info("Reading source File_PPUB_CRM_FDBK_ALL from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name='File_PPUB_CRM_FDBK_ALL')
    df_File_PPUB_CRM_FDBK_ALL_nice_rutherford = dyf.toDF().select(
        'SEGMENT_ID', 'TARGET_KEY', 'TGT_LVL_CD', 'OPERATIONALIZED_KEY', 'STAT_DT', 'STAT_TS', 'STAT_CD',
        'WEIGHT', 'DIRECTION', 'OWNING_RES_AGCY_NO', 'OWNING_RES_PDCR_NO', 'UPDATEDBY_RES_AGCY_NO',
        'UPDATEDBY_RES_PDCR_NO', 'APPLICATION_ID', 'LAST_UPDT_ID', 'LAST_UPDT_DOM', 'ORDER_ID', 'LINE_ITEM_ID',
        'OPP_ID', 'OPP_DTL_ID', 'OPP_LIB_ID', 'EXP_DT', 'PCN_STG_DT', 'PRIORITY', 'PRIORITY_STAR', 'AGENT_ADDED_IN',
        'ARM_TOUCHPOINT', 'ARM_LEAD_KEY_ID', 'ARM_LEAD_INSTANCE_ID', 'ARM_LOGICAL_SEGMENT_ID', 'LOAD_DT',
        'CurrentlyProcessedFileName'
    )
except Exception as e:
    logger.error(f"Failed reading File_PPUB_CRM_FDBK_ALL from Glue Catalog: {e}", exc_info=True)
    raise

# --- Source: File_PPUB_MLDB_OPP_DTL -------------------------------------------------
try:
    logger.info("Reading source File_PPUB_MLDB_OPP_DTL from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name='File_PPUB_MLDB_OPP_DTL')
    df_File_PPUB_MLDB_OPP_DTL_humble_shannon = dyf.toDF().select(
        'Opp_Dtl_Id', 'Opp_Lib_Id', 'Source_Cd', 'Source_Dtl_Key', 'Exp_Dt', 'Create_Dt', 'DM_URL', 'Long_Ds'
    )
except Exception as e:
    logger.error(f"Failed reading File_PPUB_MLDB_OPP_DTL from Glue Catalog: {e}", exc_info=True)
    raise

# --- SQ: SQ_File_PPUB_CRM_FDBK_ALL (passthrough + ensure alias for joiner) -------------
try:
    logger.info("Projecting SQ_File_PPUB_CRM_FDBK_ALL (passthrough + alias OPP_DTL_ID -> FDBK_ALL_OPP_DTL_ID)")
    # keep original OPP_DTL_ID and also provide alias expected by Joiner
    df_SQ_File_PPUB_CRM_FDBK_ALL_silly_noether = (
        df_File_PPUB_CRM_FDBK_ALL_nice_rutherford
        .select(
            'SEGMENT_ID', 'TARGET_KEY', 'TGT_LVL_CD', 'OPERATIONALIZED_KEY', 'STAT_DT', 'STAT_TS', 'STAT_CD',
            'WEIGHT', 'DIRECTION', 'OWNING_RES_AGCY_NO', 'OWNING_RES_PDCR_NO', 'UPDATEDBY_RES_AGCY_NO',
            'UPDATEDBY_RES_PDCR_NO', 'APPLICATION_ID', 'LAST_UPDT_ID', 'LAST_UPDT_DOM', 'ORDER_ID', 'LINE_ITEM_ID',
            'OPP_ID', 'OPP_DTL_ID', 'OPP_LIB_ID', 'EXP_DT', 'PCN_STG_DT', 'PRIORITY', 'PRIORITY_STAR', 'AGENT_ADDED_IN',
            'ARM_TOUCHPOINT', 'ARM_LEAD_KEY_ID', 'ARM_LEAD_INSTANCE_ID', 'ARM_LOGICAL_SEGMENT_ID', 'LOAD_DT',
            'CurrentlyProcessedFileName'
        )
        .withColumn('FDBK_ALL_OPP_DTL_ID', col('OPP_DTL_ID'))
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_File_PPUB_CRM_FDBK_ALL: {e}", exc_info=True)
    raise

# --- SQ: SQ_File_PPUB_MLDB_OPP_DTL (passthrough + alias to Joiner master ports) ------
try:
    logger.info("Projecting SQ_File_PPUB_MLDB_OPP_DTL (passthrough + alias Opp_Dtl_Id -> MLDB_Opp_Dtl_Id)")
    df_SQ_File_PPUB_MLDB_OPP_DTL_eager_planck = (
        df_File_PPUB_MLDB_OPP_DTL_humble_shannon
        .select('Opp_Dtl_Id', 'Opp_Lib_Id', 'Source_Cd', 'Source_Dtl_Key', 'Exp_Dt', 'Create_Dt', 'DM_URL', 'Long_Ds')
        .withColumn('MLDB_Opp_Dtl_Id', col('Opp_Dtl_Id'))
        .withColumn('MLDB_Opp_Lib_Id', col('Opp_Lib_Id'))
        .withColumn('MLDB_DM_URL', col('DM_URL'))
        .withColumn('MLDB_Source_Cd', col('Source_Cd'))
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_File_PPUB_MLDB_OPP_DTL: {e}", exc_info=True)
    raise

# --- Joiner: Join_Fdbk_All_with_MLDB_Dtl (Master Outer -> right join) -----------------
try:
    logger.info("Performing Joiner Join_Fdbk_All_with_MLDB_Dtl (Master Outer -> right join)")
    left = df_SQ_File_PPUB_CRM_FDBK_ALL_silly_noether
    right = df_SQ_File_PPUB_MLDB_OPP_DTL_eager_planck
    df_joined = left.join(right, left.FDBK_ALL_OPP_DTL_ID == right.MLDB_Opp_Dtl_Id, how='right')

    # project the Joiner output ports explicitly (preserve both original and aliased names where created)
    df_Join_Fdbk_All_with_MLDB_Dtl_nice_archimedes = df_joined.select(
        # master (MLDB) ports (aliased)
        col('MLDB_Opp_Dtl_Id'), col('MLDB_Opp_Lib_Id'), col('MLDB_DM_URL'), col('MLDB_Source_Cd'),
        # detail (FDBK) ports and passthroughs
        col('FDBK_ALL_OPP_DTL_ID'), col('FDBK_ALL_OPP_LIB_ID') if 'FDBK_ALL_OPP_LIB_ID' in df_joined.columns else col('OPP_LIB_ID').alias('FDBK_ALL_OPP_LIB_ID'),
        col('LOAD_DT'), col('SEGMENT_ID'), col('TARGET_KEY'), col('TGT_LVL_CD'), col('OPERATIONALIZED_KEY'),
        col('STAT_DT'), col('STAT_TS'), col('STAT_CD'), col('WEIGHT'), col('DIRECTION'), col('OWNING_RES_AGCY_NO'),
        col('OWNING_RES_PDCR_NO'), col('UPDATEDBY_RES_AGCY_NUM') if 'UPDATEDBY_RES_AGCY_NUM' in df_joined.columns else col('UPDATEDBY_RES_AGCY_NO').alias('UPDATEDBY_RES_AGCY_NUM'),
        col('UPDATEDBY_RES_PDCR_NUM') if 'UPDATEDBY_RES_PDCR_NUM' in df_joined.columns else col('UPDATEDBY_RES_PDCR_NO').alias('UPDATEDBY_RES_PDCR_NUM'),
        col('APPLICATION_ID'), col('LAST_UPDT_ID'), col('LAST_UPDT_DOM'), col('ORDER_ID'), col('LINE_ITEM_ID'),
        col('OPP_ID'), col('EXP_DT'), col('PCN_STG_DT'), col('PRIORITY'), col('PRIORITY_STAR'), col('AGENT_ADDED_IN'),
        col('ARM_TOUCHPOINT'), col('ARM_LEAD_KEY_ID'), col('ARM_LOGICAL_SEGMENT_ID'), col('ARM_LEAD_INSTANCE_ID'),
        col('SRC_FILE_NM_CRM_FDBK_ALL') if 'SRC_FILE_NM_CRM_FDBK_ALL' in df_joined.columns else col('CurrentlyProcessedFileName').alias('SRC_FILE_NM_CRM_FDBK_ALL')
    )
except Exception as e:
    logger.error(f"Failed during Join_Fdbk_All_with_MLDB_Dtl: {e}", exc_info=True)
    raise

# --- Expression: Exp_Generate_Source_Order_And_Interaction_Groupkeycd -----------------
try:
    logger.info("Computing Source_Order and o_ARM_LEAD_INSTANCE_ID in Exp_Generate_Source_Order_And_Interaction_Groupkeycd")
    base = df_Join_Fdbk_All_with_MLDB_Dtl_nice_archimedes

    # project passthrough columns explicitly (list mirrors downstream needs)
    passthrough_cols = [
        'OPP_ID', 'EXP_DT', 'LOAD_DT', 'SEGMENT_ID', 'TARGET_KEY', 'TGT_LVL_CD', 'OPERATIONALIZED_KEY',
        'STAT_DT', 'STAT_TS', 'STAT_CD', 'WEIGHT', 'DIRECTION', 'OWNING_RES_AGCY_NO', 'OWNING_RES_PDCR_NO',
        'UPDATEDBY_RES_AGCY_NUM', 'UPDATEDBY_RES_PDCR_NUM', 'APPLICATION_ID', 'LAST_UPDT_ID', 'LAST_UPDT_DOM',
        'ORDER_ID', 'LINE_ITEM_ID', 'OPP_DTL_ID', 'OPP_LIB_ID', 'PCN_STG_DT', 'PRIORITY', 'PRIORITY_STAR',
        'AGENT_ADDED_IN', 'ARM_TOUCHPOINT', 'ARM_LEAD_KEY_ID', 'ARM_LEAD_INSTANCE_ID', 'ARM_LOGICAL_SEGMENT_ID',
        'MLDB_DM_URL', 'SRC_FILE_NM_CRM_FDBK_ALL', 'MLDB_Opp_Dtl_Id', 'MLDB_Opp_Lib_Id'
    ]

    # keep only columns that actually exist in the dataframe to avoid AnalysisException
    final_passthrough = [c for c in passthrough_cols if c in base.columns]

    df_temp = base.select(*final_passthrough)

    # Source_Order translation of DECODE(TRUE, ...) into CASE WHEN ... THEN ... END
    source_order_expr = expr(
        "CASE WHEN STAT_CD = 'U' THEN 1 "
        "WHEN STAT_CD = 'F' THEN 2 "
        "WHEN STAT_CD IN ('C','D','I','M','N','Z') THEN 3 "
        "WHEN STAT_CD IN ('H','K','L','O','P','R','V','W','E') THEN 4 ELSE 5 END"
    )

    df_with_source_order = df_temp.withColumn('Source_Order', source_order_expr)

    # o_ARM_LEAD_INSTANCE_ID: trimmed ARM_LEAD_INSTANCE_ID or '@' when NULL/empty after trim
    df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_nifty_ramanujan = df_with_source_order.withColumn(
        'o_ARM_LEAD_INSTANCE_ID',
        when((trim(col('ARM_LEAD_INSTANCE_ID')).isNull()) | (trim(col('ARM_LEAD_INSTANCE_ID')) == ''), lit('@')).otherwise(trim(col('ARM_LEAD_INSTANCE_ID')))
    )
except Exception as e:
    logger.error(f"Failed computing Exp_Generate_Source_Order_And_Interaction_Groupkeycd: {e}", exc_info=True)
    raise

# --- Sorter: Srt_to_Generate_PE_Line_Nbr -------------------------------------------
try:
    logger.info("Sorting records in Srt_to_Generate_PE_Line_Nbr by OPP_ID, STAT_TS, Source_Order, ARM_LEAD_KEY_ID, ARM_LEAD_INSTANCE_ID, ARM_LOGICAL_SEGMENT_ID, MLDB_DM_URL, PCN_STG_DT, OPERATIONALIZED_KEY")
    sort_keys = [
        col('OPP_ID'), col('STAT_TS'), col('Source_Order'), col('ARM_LEAD_KEY_ID'), col('ARM_LEAD_INSTANCE_ID'),
        col('ARM_LOGICAL_SEGMENT_ID'), col('MLDB_DM_URL') if 'MLDB_DM_URL' in df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_nifty_ramanujan.columns else col('DM_URL'),
        col('PCN_STG_DT'), col('OPERATIONALIZED_KEY')
    ]
    # filter only keys that exist
    sort_keys_filtered = [k for k in sort_keys if (hasattr(k, 'name') and k._jc.toString() in df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_nifty_ramanujan.columns) or (not hasattr(k, 'name'))]
    # simpler: orderBy by column names that exist
    sort_col_names = [n for n in ['OPP_ID', 'STAT_TS', 'Source_Order', 'ARM_LEAD_KEY_ID', 'ARM_LEAD_INSTANCE_ID', 'ARM_LOGICAL_SEGMENT_ID', 'MLDB_DM_URL', 'PCN_STG_DT', 'OPERATIONALIZED_KEY'] if n in df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_nifty_ramanujan.columns]

    df_Srt_to_Generate_PE_Line_Nbr_mystifying_darwin = df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_nifty_ramanujan.orderBy([col(n).asc() for n in sort_col_names])
except Exception as e:
    logger.error(f"Failed sorting in Srt_to_Generate_PE_Line_Nbr: {e}", exc_info=True)
    raise

# --- Expression: Exp_Generate_PE_Line_Nbr (windowed row_number per OPP_ID) ---------
try:
    logger.info("Generating PE_Line_Nbr (row_number over partition by OPP_ID) and o_SRC_File_Nm")
    w = Window.partitionBy('OPP_ID').orderBy(
        *[col(n) for n in ['STAT_TS', 'Source_Order', 'ARM_LEAD_KEY_ID', 'ARM_LEAD_INSTANCE_ID', 'ARM_LOGICAL_SEGMENT_ID', 'MLDB_DM_URL', 'PCN_STG_DT', 'OPERATIONALIZED_KEY'] if n in df_Srt_to_Generate_PE_Line_Nbr_mystifying_darwin.columns]
    )

    # project passthrough columns first (those downstream expect them)
    proj_cols = [c for c in df_Srt_to_Generate_PE_Line_Nbr_mystifying_darwin.columns if c not in ('PE_Line_Nbr', 'o_SRC_File_Nm')]
    df_proj = df_Srt_to_Generate_PE_Line_Nbr_mystifying_darwin.select(*proj_cols)

    df_with_pe = df_proj.withColumn('PE_Line_Nbr', row_number().over(w))

    # derive o_SRC_File_Nm as the basename after the last '/'
    if 'SRC_FILE_NM_CRM_FDBK_ALL' in df_with_pe.columns:
        df_with_pe = df_with_pe.withColumn('o_SRC_File_Nm', element_at(split(col('SRC_FILE_NM_CRM_FDBK_ALL'), '/'), -1))
    else:
        # fallback to CurrentlyProcessedFileName if present
        if 'CurrentlyProcessedFileName' in df_with_pe.columns:
            df_with_pe = df_with_pe.withColumn('o_SRC_File_Nm', element_at(split(col('CurrentlyProcessedFileName'), '/'), -1))

    df_Exp_Generate_PE_Line_Nbr_careful_leibniz = df_with_pe
except Exception as e:
    logger.error(f"Failed computing Exp_Generate_PE_Line_Nbr: {e}", exc_info=True)
    raise

# assign the Output-node dataframe name expected by downstream/validation
try:
    logger.info("Assigning df_File_PPUB_CRM_FDBK_ALL_Processed_cool_archimedes from final expression dataframe")
    df_File_PPUB_CRM_FDBK_ALL_Processed_cool_archimedes = df_Exp_Generate_PE_Line_Nbr_careful_leibniz
except Exception as e:
    logger.error(f"Failed assigning File_PPUB_CRM_FDBK_ALL_Processed dataframe: {e}", exc_info=True)
    raise

# --- Output: File_PPUB_CRM_FDBK_ALL_Processed (flat file write, single-part) -------
try:
    logger.info("Writing flat-file target File_PPUB_CRM_FDBK_ALL_Processed as single delimited file")
    tmp_dir = os.path.join(PM_TARGET_FILE_DIR, '_tmp_File_PPUB_CRM_FDBK_ALL_Processed')
    target_filename = 'PE_Preprocess_Outupt.out'
    target_path = os.path.join(PM_TARGET_FILE_DIR, target_filename)

    # ensure clean staging
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    # write as single-part delimited file using Spark staging
    df_File_PPUB_CRM_FDBK_ALL_Processed_cool_archimedes.coalesce(1).write.mode('overwrite').option('header', False).option('sep', '\x04').csv(tmp_dir)

    # locate the single part file and move it to the exact target path
    part_files = glob.glob(os.path.join(tmp_dir, 'part-*'))
    if not part_files:
        raise FileNotFoundError(f"No part file produced in tmp_dir {tmp_dir}")
    part_file = part_files[0]

    # ensure target directory exists
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    if os.path.exists(target_path):
        os.remove(target_path)

    shutil.move(part_file, target_path)
    # cleanup staging
    shutil.rmtree(tmp_dir)
    logger.info(f"Wrote flat file to {target_path}")
except Exception as e:
    logger.error(f"Failed writing File_PPUB_CRM_FDBK_ALL_Processed flat file: {e}", exc_info=True)
    raise



job.commit()
