import re


to_snake_case = lambda __s=str(): re.sub(r"(?<!^)(?=[A-Z])", '_', str(__s)).lower()
to_camel_case = lambda __s=str(): ''.join([s.capitalize() if __i > 0 else s for __i, s in enumerate(str(__s).split('_'))])


def exists_context(**context):
    return {key: value for key, value in context.items() if value or isinstance(value, (bool,float,int))}


UNIQUE_CONTEXT = lambda asyncio=False, operation=None, host=None, where=None, which=None, initTime=None, \
                        contextFields=None, iterateArgs=None, iterateQuery=None, iterateUnit=None, fromNow=None, \
                        responseType=None, logger=None, logJson=None, errors=None, rename=None, schemaInfo=None, \
                        redirectArgs=None, redirectQuery=None, redirectUnit=None, \
                        maxLimit=None, redirectLimit=None, authClass=None, dependencies=None, \
                        data=None, results=None, crawler=None, prefix=None, self_var=True, **context: context


REQUEST_CONTEXT = lambda session=None, semaphore=None, page=None, method=None, url=None, \
                        params=None, encode=None, data=None, json=None, headers=None, allow_redirects=None, close=None, \
                        validate=None, exception=None, valid=None, invalid=None, bytes=None, engine=None, **context: context


PROXY_CONTEXT = lambda fields=list(), iterateUnit=None, interval=None, startDate=None, endDate=None, \
                        datetimeUnit="second", tzinfo=None, returnType=None, \
                        logName=str(), logLevel="WARN", logFile=str(), debug=False, rename=dict(), \
                        delay=1., numTasks=100, progress=True, message=str(), apiRedirect=False, \
                        queryInfo=None, uploadInfo=None, encryptedKey=None, decryptedKey=None, cookies=str(), **context: \
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
        progress = progress,
        message = message,
        apiRedirect = apiRedirect,
        encryptedKey = encryptedKey,
        cookies = cookies,
        ), **context)


REDIRECT_CONTEXT = lambda apiRedirect=None, logFile=None, **context: PROXY_CONTEXT(**UNIQUE_CONTEXT(**context))
