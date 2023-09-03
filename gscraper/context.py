def exists_context(**context):
    return {key: value for key, value in context.items() if value or isinstance(value, (bool,float,int))}


CONFIG_CONTEXT = lambda asyncio=False, operation=None, host=None, where=None, which=None, initTime=None, \
                        session=None, semaphore=None, fields=list(), contextFields=None, \
                        iterateArgs=None, iterateQuery=None, iterateUnit=None, interval=None, \
                        startDate=None, endDate=None, datetimeUnit="second", tzinfo=None, \
                        responseType=None, returnType=None, logName=str(), logLevel="WARN", logFile=str(), \
                        logJson=False, logger=None, debug=False, errors=None, rename=dict(), \
                        root=None, schemaInfo=None, delay=1., numTasks=100, maxLimit=None, redirectLimit=None, \
                        progress=True, message=str(), apiRedirect=False, reidrectUnit=1, redirectErrors=False, \
                        queryKey=None, querySheet=None, queryFields=None, queryString=None, queryArray=None, \
                        authType=None, idKey=None, pwKey=None, extraKeys=None, decryptedKey=None, dependencies=None, \
                        data=None, results=None, crawler=None, prefix=None, self_var=True, **context: \
    dict(exists_context(
        fields = fields,
        startDate = startDate,
        endDate = endDate,
        datetimeUnit = datetimeUnit,
        tzinfo = tzinfo,
        logName = logName,
        logLevel = logLevel,
        logFile = logFile,
        debug = debug,
        rename = rename,
        delay = delay,
        numTasks = numTasks,
        redirectLimit = redirectLimit,
        progress = progress,
        message = message,
        apiRedirect = apiRedirect,
        reidrectUnit = reidrectUnit,
        redirectErrors = redirectErrors,
        ), **context)


FIXED_CONTEXT = lambda operation=None, where=None, which=None, initTime=None, contextFields=None, \
                        iterateArgs=None, iterateQuery=None, redirectQuery=None, redirectUnit=None, \
                        responseType=None, logger=None, logJson=None, errors=None, maxLimit=None, redirectLimit=None, \
                        rename=None, authClass=None, idKey=None, pwKey=None, decryptedKey=None, \
                        dependencies=None, **context: context


ENC_CONTEXT = lambda encryptedKey=str(), decryptedKey=None, cookies=str(), userid=None, passwd=None, \
                    **context: \
    dict(exists_context(
        encryptedKey = encryptedKey,
        cookies = cookies,
        ), **context)


UPLOAD_CONTEXT = lambda gsKey=None, gsSheet=None, gsMode=None, gsBaseSheet=None, gsRange=None, \
                    gbqPid=None, gbqTable=None, gbqMode=None, gbqSchema=None, gbqProgress=True, \
                    gbqPartition=None, gbqPartitionBy=None, gbqReauth=False, \
                    **context: context


GS_CONTEXT = lambda key=None, sheet=None, mode=None, base_sheet=None, cell=None, clear=None, **context: context


GBQ_CONTEXT = lambda table=None, project_id=None, mode=None, schema=None, progress=None, \
                    partition=None, partition_by=None, reauth=None, **context: context


PROXY_CONTEXT = lambda **context: CONFIG_CONTEXT(**UPLOAD_CONTEXT(**context))


REDIRECT_CONTEXT = lambda apiRedirect=None, redirectUnit=None, redirectErrors=None, logFile=None, **context: \
    CONFIG_CONTEXT(**ENC_CONTEXT(**UPLOAD_CONTEXT(**context)))


GCP_CONTEXT = lambda **context: GS_CONTEXT(**GBQ_CONTEXT(**context))
