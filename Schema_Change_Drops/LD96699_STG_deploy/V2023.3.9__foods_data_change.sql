delete from rollback.foods where code in ('CHR', 'CHK');
insert into rollback.foods values('MAM', 'M&M');
insert into rollback.foods values('PIE', 'pie');
