def exists_context(**context):
    return {key: value for key, value in context.items() if value or isinstance(value, (bool,float,int))}


CONFIG_CONTEXT = lambda asyncio=False, operation=str(), host=str(), where=str(), which=str(), initTime=None, \
                        session=None, semaphore=None, filter=list(), filterContext=None, \
                        contextArgs=list(), contextQuery=list(), contextUnit=1, contextPeriod=str(), \
                        startDate=None, endDate=None, returnType=str(), \
                        logName=str(), logLevel="WARN", logFile=str(), logJson=False, logger=None, \
                        logErrors=False, errorArgs=tuple(), errorKwargs=tuple(), errors=dict(), \
                        delay=1., numTasks=100, progress=True, debug=False, message=str(), rename=dict(), \
                        apiRedirect=False, reidrectUnit=1, redirectErrors=False, localSave=False, extraSave=False, \
                        queryKey=str(), querySheet=str(), queryFields=list(), queryString=list(), queryArray=list(), \
                        authType=None, idKey=None, pwKey=None, extraKeys=None, dependencies=list(), \
                        data=None, results=None, crawler=None, prefix=str(), self_var=True, **context: \
    dict(exists_context(
        filter = filter,
        startDate = startDate,
        endDate = endDate,
        logName = logName,
        logLevel = logLevel,
        logFile = logFile,
        logErrors = logErrors,
        errorArgs = errorArgs,
        errorKwargs = errorKwargs,
        delay = delay,
        numTasks = numTasks,
        progress = progress,
        debug = debug,
        message = message,
        rename = rename,
        apiRedirect = apiRedirect,
        reidrectUnit = reidrectUnit,
        redirectErrors = redirectErrors,
        ), **context)


ENC_CONTEXT = lambda encryptedKey=str(), decryptedKey=dict(), cookies=str(), userid=str(), passwd=str(), \
                    **context: \
    dict(exists_context(
        encryptedKey = encryptedKey,
        cookies = cookies,
        ), **context)


UPLOAD_CONTEXT = lambda gsKey=str(), gsSheet=str(), gsMode="append", gsBaseSheet=str(), gsRange=str(), \
                    gbqPid=str(), gbqTable=str(), gbqMode="append", gbqSchema=None, gbqProgress=True, \
                    gbqPartition=str(), gbqPartitionBy="auto", gbqReauth=False, \
                    **context: context


GS_CONTEXT = lambda key=str(), sheet=str(), mode="append", base_sheet=str(), cell=str(), clear=False, **context: context


GBQ_CONTEXT = lambda table=str(), project_id=str(), mode="append", schema=None, progress=True, \
                    partition=str(), partition_by="auto", reauth=False, **context: context


PROXY_CONTEXT = lambda **context: CONFIG_CONTEXT(**UPLOAD_CONTEXT(**context))


REDIRECT_CONTEXT = lambda apiRedirect=False, redirectUnit=(1,), redirectErrors=False, logFile=str(), **context: \
    CONFIG_CONTEXT(**ENC_CONTEXT(**UPLOAD_CONTEXT(**context)))


GCP_CONTEXT = lambda **context: GS_CONTEXT(**GBQ_CONTEXT(**context))
