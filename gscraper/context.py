import re


to_snake_case = lambda __s=str(): re.sub(r"(?<!^)(?=[A-Z])", '_', str(__s)).lower()
to_camel_case = lambda __s=str(): ''.join([s.capitalize() if __i > 0 else s for __i, s in enumerate(str(__s).split('_'))])


def exists_context(**context):
    return {key: value for key, value in context.items() if value or isinstance(value, (bool,float,int))}


UNIQUE_CONTEXT = lambda self=None, asyncio=False, operation=None, host=None, where=None, which=None, \
                        initTime=None, contextFields=None, iterateArgs=None, iterateQuery=None, \
                        pagination=None, pageUnit=None, pageLimit=None, responseType=None, \
                        logger=None, logJson=None, errors=None, schemaInfo=None, \
                        redirectArgs=None, redirectQuery=None, maxLimit=None, redirectLimit=None, \
                        authClass=None, dependencies=None, data=None, results=None, crawler=None, \
                        prefix=None, self_var=True, **context: context


TASK_CONTEXT = lambda params=None, locals=None, how=None, default=None, dropna=None, strict=None, unique=None, \
                        to=None, **context: UNIQUE_CONTEXT(**context)


REQUEST_CONTEXT = lambda session=None, semaphore=None, method=None, url=None, \
                        params=None, encode=None, data=None, json=None, headers=None, allow_redirects=None, \
                        close=None, validate=None, exception=None, valid=None, invalid=None, html=None, \
                        table_header=None, table_idx=None, engine=None, how=None, default=None, \
                        dropna=None, strict=None, unique=None, to=None, **context: TASK_CONTEXT(**context)


PROXY_CONTEXT = lambda fields=list(), iterateUnit=0, interval=None, fronNow=None, \
                        startDate=None, endDate=None, datetimeUnit="second", tzinfo=None, returnType=None, \
                        logName=str(), logLevel="WARN", logFile=str(), debug=False, renameMap=dict(), \
                        delay=1., progress=True, message=str(), numTasks=100, apiRedirect=False, redirectUnit=0, \
                        queryInfo=None, uploadInfo=None, encryptedKey=None, decryptedKey=None, cookies=str(), **context: \
    dict(exists_context(
        fields = fields,
        iterateUnit = iterateUnit,
        interval = interval,
        fronNow = fronNow,
        startDate = startDate,
        endDate = endDate,
        datetimeUnit = datetimeUnit,
        tzinfo = tzinfo,
        returnType = returnType,
        logName = logName,
        logLevel = logLevel,
        logFile = logFile,
        debug = debug,
        renameMap = renameMap,
        delay = delay,
        numTasks = numTasks,
        progress = progress,
        message = message,
        apiRedirect = apiRedirect,
        redirectUnit = redirectUnit,
        encryptedKey = encryptedKey,
        cookies = cookies,
        ), **context)


REDIRECT_CONTEXT = lambda apiRedirect=None, returnType=None, renameMap=None, logFile=None, **context: \
    PROXY_CONTEXT(**REQUEST_CONTEXT(**context))
