from aws_cdk import (
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_events as events,
    aws_iam as iam,
    aws_lambda_event_sources,
    aws_events_targets as targets,
    aws_s3_notifications,
    aws_kinesisfirehose,
    aws_lambda as _lambda,
    aws_glue as glue,
    core
)


class PortalInmobiliarioScannerStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        REGION_NAME = 'us-west-1'
        BUCKET_NAME = 'data_lake'
        SEARCHES_PREFIX = '0-search'

        RESULTS_PREFIX = '1-results'

        LAYER_PANDAS_ARN       = 'arn:aws:lambda:us-west-1:844626608976:layer:Pandas:1'
        LAYER_SELENIUM_ARN     = 'arn:aws:lambda:us-west-1:844626608976:layer:selenium:1'
        LAYER_CHROMEDRIVER_ARN = 'arn:aws:lambda:us-west-1:844626608976:layer:chromedriver:1'
        LAYER_S3FS_ARN         = 'arn:aws:lambda:us-west-1:844626608976:layer:s3fs:1'


        layer_pandas         = _lambda.LayerVersion.from_layer_version_arn(self, "layer_pandas", LAYER_PANDAS_ARN)
        layer_s3fs           = _lambda.LayerVersion.from_layer_version_arn(self, "layer_s3fs", LAYER_S3FS_ARN)
        layer_selenium       = _lambda.LayerVersion.from_layer_version_arn(self, "layer_selenium", LAYER_SELENIUM_ARN)
        layer_chromedriver   = _lambda.LayerVersion.from_layer_version_arn(self, "layer_chromedriver", LAYER_CHROMEDRIVER_ARN)


# ================================================================================================================
# Bucket de Trabajo
# ================================================================================================================


        bucket = s3.Bucket(self, BUCKET_NAME ,  versioned=False)



# ================================================================================================================
#  Cola para busquedas
# ================================================================================================================

        queue_fail = sqs.Queue(self, "searches_failed_", visibility_timeout=core.Duration.seconds(30))
        dlq = sqs.DeadLetterQueue( max_receive_count=3, queue=queue_fail)
        queue_searches = sqs.Queue(self, "searches", visibility_timeout=core.Duration.seconds(900), dead_letter_queue=dlq)

# ================================================================================================================
# Lambda genera mensajes para busquedas
# ================================================================================================================


        make_search_queue = _lambda.Function(self, "make_queue_", runtime=_lambda.Runtime.PYTHON_3_6,
                                      handler="lambda-handler.main", timeout=core.Duration.seconds(60),
                                      memory_size=256, code=_lambda.Code.asset("./lambda/make-search-queue"),
                                      description='Genera la cola de mensajes para las búsquedas', 
                                      layers=[layer_pandas, layer_s3fs], environment={
                                          'ENV_REGION_NAME': REGION_NAME,
                                          'ENV_S3_BUCKET': bucket.bucket_name,
                                          'ENV_S3_PREFIX': SEARCHES_PREFIX,
                                          'ENV_SQS_QUEUE':queue_searches.queue_url
                                      })

        bucket.grant_read_write(make_search_queue)
        queue_searches.grant_send_messages(make_search_queue)

        event_rule_day = events.Rule(self, "cada-dia",schedule=events.Schedule.cron(day='*',hour='22'))
        event_rule_day.add_target(targets.LambdaFunction(make_search_queue))

# ================================================================================================================
# FUNCION WEBSCRAPPER
# ================================================================================================================



        web_scrapper_function = _lambda.Function(self, "web_scrapper_", runtime=_lambda.Runtime.PYTHON_3_6,
                                      handler="lambda-handler.main", timeout=core.Duration.seconds(900),
                                      memory_size=3008, code=_lambda.Code.asset("./lambda/scrap-portal-inmobiliario"),
                                      reserved_concurrent_executions = 10,
                                      description='Realiza una búsqueda en Portal Inmobiliario de acuerdo a ciertas condiciones', 
                                      layers=[layer_pandas, layer_selenium, layer_chromedriver], environment={
                                          'ENV_REGION_NAME': REGION_NAME,
                                          'ENV_S3_BUCKET': bucket.bucket_name,
                                          'ENV_S3_PREFIX': RESULTS_PREFIX,
                                          'ENV_SQS_QUEUE':queue_searches.queue_url
                                      })

        bucket.grant_read_write(web_scrapper_function)
        queue_searches.grant_consume_messages(web_scrapper_function)
        
        event_rule = events.Rule(self, "cada-minuto",schedule=events.Schedule.cron(minute='*/5'))
        event_rule.add_target(targets.LambdaFunction(web_scrapper_function))



# ================================================================================================================
# GLUE DATABASE
# ================================================================================================================

        glue_db = glue.Database(self, "portal_inmobiliatio",database_name="pi")

        statement = iam.PolicyStatement(actions=["s3:GetObject","s3:PutObject"],
                                        resources=["arn:aws:s3:::{}".format(bucket.bucket_name),"arn:aws:s3:::{}/*".format(bucket.bucket_name)])
        write_to_s3_policy = iam.PolicyDocument(statements=[statement])
        glue_role = iam.Role(
            self, 'GlueCrawlerPortalInmobiliarioRole',
            role_name = 'GlueCrawlerPortalInmobiliarioRole',
            inline_policies=[write_to_s3_policy],
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies = [ iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')]
        )

        glue_crawler = glue.CfnCrawler(
            self, 'glue-crawler-pi-results',
            description="Glue Crawler para los resultados",
            name='pi-results-crawler',
            database_name=glue_db.database_name,
            schedule=None,
            role=glue_role.role_arn,
            targets={"s3Targets": [{"path": "s3://{}/{}/".format(bucket.bucket_name, RESULTS_PREFIX)}]}
        )

