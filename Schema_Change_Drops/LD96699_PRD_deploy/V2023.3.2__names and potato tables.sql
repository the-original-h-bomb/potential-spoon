create table ROLLBACK.NAMES
(
    ID   NUMBER not null
        primary key,
    NAME VARCHAR(255),
    CODE VARCHAR(255)
);

create table ROLLBACK.POTATO_FRANCHISE
(
    ID             NUMBER,
    POTATO_TYPE    VARCHAR(50),
    POTATO_PRODUCT VARCHAR(50)
);

