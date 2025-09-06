-- see everything, but up to five rows
SELECT *
FROM ZWAMESSAGE
LIMIT 5
;

-- specific column
SELECT ZTEXT
FROM ZWAMESSAGE
LIMIT 5
;

-- there are "empty messages" for whatever reason
SELECT ZTEXT
FROM ZWAMESSAGE
WHERE
    ZTEXT IS NOT NULL
LIMIT 5
;

-- see other columns
SELECT ZTEXT
    , ZMESSAGEDATE -- like Unix time but starting 01/01/2001
    , ZSENTDATE -- ditto
    , ZFROMJID -- who sent the message?
    , ZTOJID -- to whom is the message sent?
FROM ZWAMESSAGE
WHERE
    ZTEXT IS NOT NULL
LIMIT 5
;

-- get all messages someone sent you personally
SELECT ZTEXT
    , ZMESSAGEDATE
    , ZSENTDATE
FROM ZWAMESSAGE
WHERE
    ZTEXT IS NOT NULL
    AND
    ZFROMJID = "628123456789@s.whatsapp.net" -- from phone number
    AND
    ZTOJID IS NULL -- to "no one" = yourself, I guess
ORDER BY ZMESSAGEDATE ASC -- chronological order, hopefully
LIMIT 5
;

-- YouTube links specifically
SELECT ZTEXT
    , ZMESSAGEDATE
    , ZSENTDATE
FROM ZWAMESSAGE
WHERE
    ZTEXT LIKE "https://youtu%" -- catch "youtu.be" and "youtube.com"
    AND
    ZFROMJID = "628123456789@s.whatsapp.net"
    AND
    ZTOJID IS NULL
ORDER BY ZMESSAGEDATE ASC
LIMIT 5
;
