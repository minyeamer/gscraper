UNIQUE_CONTEXT = lambda self=None, asyncio=None, operation=None, host=None, where=None, which=None, by=None, \
                        initTime=None, contextFields=None, iterateArgs=None, iterateProduct=None, \
                        pagination=None, pageUnit=None, pageLimit=None, fromNow=None, \
                        debug=None, localSave=None, extraSave=None, interrupt=None, \
                        logger=None, logJson=None, errors=None, ssl=None, \
                        redirectArgs=None, redirectProduct=None, maxLimit=None, redirectLimit=None, \
                        root=None, groupby=None, rankby=None, schemaInfo=None, \
                        crawler=None, decryptedKey=None, auth=None, sessionCookies=None, dependencies=None, \
                        inplace=None, self_var=None, prefix=None, rename=None, **context: context


TASK_CONTEXT = lambda locals=None, how=None, default=None, dropna=None, strict=None, unique=None, \
                        drop=None, index=None, count=None, to=None, **context: UNIQUE_CONTEXT(**context)


REQUEST_CONTEXT = lambda session=None, semaphore=None, method=None, url=None, referer=None, messages=None, \
                        params=None, encode=None, data=None, json=None, headers=None, cookies=None, \
                        allow_redirects=None, validate=None, exception=None, valid=None, invalid=None, \
                        close=None, encoding=None, features=None, html=None, table_header=None, table_idx=None, \
                        engine=None, **context: TASK_CONTEXT(**context)


LOGIN_CONTEXT = lambda userid=None, passwd=None, domain=None, naverId=None, naverPw=None, \
                        **context: REQUEST_CONTEXT(**context)


API_CONTEXT = lambda clientId=None, clientSecret=None, **context: REQUEST_CONTEXT(**context)


RESPONSE_CONTEXT = lambda iterateUnit=None, logName=None, logLevel=None, logFile=None, \
                        delay=None, progress=None, message=None, numTasks=None, apiRedirect=None, \
                        redirectUnit=None, index=0, **context: \
                        dict(REQUEST_CONTEXT(**context), index=index)


SCHEMA_CONTEXT = lambda schema=None, root=None, by=None, count=None, discard=None, name=None, path=None, \
                        type=None, mode=None, description=None, cast=None, strict=None, default=None, \
                        apply=None, match=None, how=None, query=None, **context: context


UPLOAD_CONTEXT = lambda name=None, key=None, sheet=None, mode=None, cell=None, base_sheet=None, clear=None, \
                        default=None, head=None, headers=None, numericise_ignore=None, str_cols=None, arr_cols=None, \
                        to=None, rename=None, table=None, project_id=None, schema=None, base_query=None, \
                        progress=None, partition=None, prtition_by=None, base=None, **context: context


PROXY_CONTEXT = lambda queryInfo=None, uploadInfo=None, reauth=None, audience=None, credentials=None, \
                        **context: context


REDIRECT_CONTEXT = lambda apiRedirect=None, returnType=None, logFile=None, **context: \
    PROXY_CONTEXT(**REQUEST_CONTEXT(**context))
