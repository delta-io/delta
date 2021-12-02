from argparse import Namespace

from laplace_spark.app_abc.base_smartstore import SparkBaseAppSmartstore
from laplace_spark.constants import DATE_ID_COLUMN_NAME
from laplace_spark.modules.provider import Provider  # type: ignore
from laplace_spark.transformation_factory.base import TransformationFactory
from laplace_spark.utils.etl_step import ETLStep
from laplace_spark.utils.spark_session import get_spark_session
from pyspark.sql import DataFrame

TRANSFORMATION = (
    TransformationFactory()
    .provider(Provider.SMARTSTORE)
    .etl_step(ETLStep.LOG_0)
    .build()
)


class SparkAppSmartstoreSrcToLog0(SparkBaseAppSmartstore):
    transformation = TRANSFORMATION

    def get_src_path(self, args: Namespace) -> str:
        path_prefix = SparkAppSmartstoreSrcToLog0.get_path_prefix(
            mall_id=args.mall_id,
            login_type=args.login_type,
            mall_name=args.mall_name,
            data_category=args.data_category,
        )

        return f"{path_prefix}/sourcing"

    def get_dest_path(self, args: Namespace) -> str:
        path_prefix = SparkAppSmartstoreSrcToLog0.get_path_prefix(
            mall_id=args.mall_id,
            login_type=args.login_type,
            mall_name=args.mall_name,
            data_category=args.data_category,
        )

        return f"{path_prefix}/log0"

    def read(self, path: str) -> DataFrame:
        return self.spark_session.read.format("delta").load(path)

    def write(self, path: str, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy(DATE_ID_COLUMN_NAME).save(path)


if __name__ == "__main__":  # pragma: no cover
    with get_spark_session("SparkAppSmartstoreSrcToLog0") as spark_session:
        spark_app_src_to_log0 = SparkAppSmartstoreSrcToLog0(spark_session=spark_session)
        spark_app_src_to_log0.run()
