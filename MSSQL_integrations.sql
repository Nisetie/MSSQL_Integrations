USE [master]
GO
CREATE DATABASE [Integrations];
GO
USE [Integrations];
GO

CREATE TABLE [dbo].[LogDetails](
	[id] [bigint] IDENTITY(1,1) primary key NOT NULL,
	[log_id] [bigint] NULL,
	[Details] [xml] NOT NULL
);
GO

CREATE TABLE [dbo].[Log](
	[id] [bigint] IDENTITY(1,1) primary key NOT NULL,
	[Timestamp] [datetime] default (getdate()) NULL,
	[DataObject_id] [int] NOT NULL,
	[Command] [nvarchar](max) NULL,
	[IsError] [bit] NULL
);
GO

CREATE view [dbo].[v_Log] as 
with [all] as (
SELECT l.id, l.DataObject_id, l.Command, l.IsError,  ld.details.exist(N'/INSERT') [INSERT], ld.details.exist(N'/DELETE') [DELETE],ld.details.exist(N'/UPDATE') [UPDATE]
FROM [Integrations].[dbo].[Log] l with (nolock)
left join [Integrations].[dbo].LogDetails ld with (nolock) on l.id = ld.log_id
)
,inserted as (
SELECT id, DataObject_id, Command, IsError,  count(id) as [INSERT]
from [all]
where [INSERT] = 1
group by id, DataObject_id, Command, IsError
)
,deleted as (
SELECT id, DataObject_id, Command, IsError,  count(id) as [DELETE]
from [all]
where [DELETE] = 1
group by id, DataObject_id, Command, IsError
)
,updated as (
SELECT id, DataObject_id, Command, IsError,  count(id) as [UPDATE]
from [all]
where [UPDATE] = 1
group by id, DataObject_id, Command, IsError
)
,errors as (
SELECT id, DataObject_id, Command, IsError
FROM [all]
where IsError = 1
)
select l.id, l.Timestamp, l.DataObject_id, l.Command, errors.IsError, inserted.[INSERT], deleted.[DELETE], updated.[UPDATE] 
from dbo.[Log] l 
left join inserted on l.id = inserted.id
left join deleted on l.id = deleted.id
left join updated on l.id = updated.id
left join errors on l.id = errors.id
GO

CREATE TABLE [dbo].[DataAttributes](
	[id] [int] IDENTITY(1,1) primary key NOT NULL,
	[DataObject_id] [int] NOT NULL,
	[SourceName] [nvarchar](max) NOT NULL,
	[DestinationName] [nvarchar](max) NULL,
	[IsUnique] [bit] default(0) NOT NULL,
	[IgnoreCompare] [bit] default(0) NOT NULL,
	[IsTimestamp] [bit] default (0) NOT NULL,
	[description] [nvarchar](max) default('') NULL
);
GO

CREATE TABLE [dbo].[DataObject](
	[id] [int] IDENTITY(1,1) primary key NOT NULL,
	[name] [nvarchar](max) NOT NULL,
	[description] [nvarchar](max) default('') NULL,
	[SourceDefinition] [nvarchar](max) NULL,
	[SourceName] [nvarchar](max) NULL,
	[DestinationName] [nvarchar](max) NOT NULL,
	[CompareMode] [int] default (1) NOT NULL,
	[MergeInsert] [bit] default (1) NOT NULL,
	[MergeUpdate] [bit] default (1) NOT NULL,
	[MergeDelete] [bit] default (1) NOT NULL,
	[LogChanges] [bit] default (1) NOT NULL
);
GO

/*
Генерация sql-кода для запроса данных из источника и их загрузки на сторону приемника с помощью MERGE.
Возможности генерации:
- можно записать в колонку DataObject.Action произвольные CTE-запросы для [source] и [target]
- при вызове через sp_executesql параметры @StartDatetime, @EndDateTime можно использовать в запросах DataObject.Action, чтобы
	автоматически применять фильтрация по времени
*/
-- select [dbo].[usp_generate](11,1)
CREATE function [dbo].[usp_generate] (@dataObjectId int, @useAction bit = 0)
returns nvarchar(max)
as begin

	if @dataObjectId is null or not exists (select id from dbo.DataObject where id = @dataObjectId) begin
		return null;
	end

	if @useAction is null set @useAction = 0;

	/* METADATA */
	DECLARE 
		@compareMode int, -- 1 - compare all columns keys; 2 - compare only keys
		@mergeInsert bit,
		@mergeUpdate bit,
		@mergeDelete bit,
		@logChanges bit,
		@srcDOName nvarchar(max),
		@srcFields nvarchar(max) = '',
		@srcFieldsUnique nvarchar(max) = '',
		@srcFieldsTimestamp nvarchar(max) = '',
		@srcFieldsTimestampFilter nvarchar(max) = '',
		@dstDOName nvarchar(max),
		@dstFields nvarchar(max) = '',
		@dstFieldsUnique nvarchar(max) = '',
		@dstFieldsTimestamp nvarchar(max) = '',
		@dstFieldsTimestampFilter nvarchar(max) = '',
		@mergeOn nvarchar(max) = '',
		@updateFields nvarchar(max) = '',
		@resultOldFields nvarchar(max) = '',
		@resultOldFieldsSelect nvarchar(max) = '',
		@resultNewFields nvarchar(max) = '',
		@resultNewFieldsSelect nvarchar(max) = '',
		@insertedFields nvarchar(max) = '',
		@deletedFields nvarchar(max) = '';

	select
		@compareMode = CompareMode
		,@mergeInsert = MergeInsert
		,@mergeUpdate = MergeUpdate
		,@mergeDelete = MergeDelete
		,@logChanges = LogChanges
	from dbo.DataObject 
	where DataObject.id = @dataObjectId;

	select 
		@srcDOName =SourceName
		,@dstDOName = DestinationName
	from dbo.DataObject 
	where id = @dataObjectId;
	
	select 
		@srcFields += QUOTENAME(SourceName)+',
'
		,@dstFields += QUOTENAME(isnull(DestinationName,SourceName))+',
'
		,@updateFields += concat('[target].',QUOTENAME(isnull(DestinationName,SourceName)),'=[source].',QUOTENAME(SourceName))+',
'
		,@resultOldFields += concat(QUOTENAME(concat('Old_',isnull(DestinationName,SourceName))),' ', iif (metaData.max_length=-1,'nvarchar(64)',system_type_name),',
')
		,@resultNewFields += concat(QUOTENAME(concat('New_',isnull(DestinationName,SourceName))),' ',iif (metaData.max_length=-1,'nvarchar(64)',system_type_name),',
')
		,@resultNewFieldsSelect += concat(QUOTENAME(concat('New_',isnull(DestinationName,SourceName))),' as ',QUOTENAME(isnull(DestinationName,SourceName)),',
')
		,@resultOldFieldsSelect += concat(QUOTENAME(concat('Old_',isnull(DestinationName,SourceName))),' as ',QUOTENAME(isnull(DestinationName,SourceName)),',
')
		,@insertedFields += iif (metaData.max_length=-1,concat('convert(nvarchar(64),hashbytes(''sha2_512'',','[inserted].',QUOTENAME(isnull(DestinationName,SourceName))) + '),2)',concat('[inserted].',QUOTENAME(isnull(DestinationName,SourceName)))) + ',
'
		,@deletedFields += iif (metaData.max_length=-1,concat('convert(nvarchar(64),hashbytes(''sha2_512'',','[deleted].',QUOTENAME(isnull(DestinationName,SourceName))) + '),2)',concat('[deleted].',QUOTENAME(isnull(DestinationName,SourceName)))) + ',
'
	from dbo.DataAttributes 
	join (select * from sys.dm_exec_describe_first_result_set(concat('select top 1 * from ',@dstDOName),null,null)) as metaData 
		on lower(isnull(DataAttributes.DestinationName,DataAttributes.SourceName)) = lower(metaData.[name])
	where DataAttributes.DataObject_id = @dataObjectId;

	IF len(@srcFields) = 0 begin  return null; end ELSE BEGIN
		SET @srcFields = LEFT(@srcFields, len(@srcFields)-3);
		SET @dstFields = LEFT(@dstFields, len(@dstFields)-3);
		SET @updateFields = LEFT(@updateFields, len(@updateFields)-3);
		SET @resultOldFields = LEFT(@resultOldFields, len(@resultOldFields)-3);
		SET @resultNewFields = LEFT(@resultNewFields, len(@resultNewFields)-3);
		SET @insertedFields = LEFT(@insertedFields, len(@insertedFields)-3);
		SET @deletedFields = LEFT(@deletedFields, len(@deletedFields)-3);
		SET @resultNewFieldsSelect = LEFT(@resultNewFieldsSelect, len(@resultNewFieldsSelect)-3);
		SET @resultOldFieldsSelect = LEFT(@resultOldFieldsSelect, len(@resultOldFieldsSelect)-3);
	END
		
	SELECT
		@srcFieldsUnique += QUOTENAME(SourceName)+','
		,@dstFieldsUnique += QUOTENAME(isnull(DestinationName,SourceName))+','
	FROM dbo.DataAttributes 
	WHERE DataAttributes.DataObject_id = @dataObjectId and IsUnique = 1;

	IF len(@srcFieldsUnique) > 0 BEGIN
		set @srcFieldsUnique = LEFT(@srcFieldsUnique, len(@srcFieldsUnique)-1);
		set @dstFieldsUnique = LEFT(@dstFieldsUnique, len(@dstFieldsUnique)-1);	
	END
	
	select 
		@srcFieldsTimestamp += QUOTENAME(SourceName)+' DESC,'
		,@srcFieldsTimestampFilter += concat('(',QUOTENAME(SourceName),' >= @startDateTime and ',QUOTENAME(SourceName),' <= @endDateTime)
OR ')
		,@dstFieldsTimestampFilter += concat('(',QUOTENAME(isnull(DestinationName, SourceName)),' >= @startDateTime and ',QUOTENAME(isnull(DestinationName, SourceName)),' <= @endDateTime)
OR ')
	from dbo.DataAttributes 
	where DataAttributes.DataObject_id = @dataObjectId and IsTimestamp = 1

	if len(@srcFieldsTimestampFilter) = 0 set @srcFieldsTimestampFilter = '1=1';
	else set @srcFieldsTimestampFilter = LEFT(@srcFieldsTimestampFilter, len(@srcFieldsTimestampFilter)-3);

	if len(@dstFieldsTimestampFilter) = 0 set @dstFieldsTimestampFilter = '1=1';
	else set @dstFieldsTimestampFilter = LEFT(@dstFieldsTimestampFilter, len(@dstFieldsTimestampFilter)-3);

	if len(@srcFieldsTimestamp) > 0 set @srcFieldsTimestamp = left(@srcFieldsTimestamp, len(@srcFieldsTimestamp)-1);
	if len(@dstFieldsTimestamp) > 0 set @dstFieldsTimestamp = left(@dstFieldsTimestamp, len(@dstFieldsTimestamp)-1);

	SELECT 
		@mergeOn += concat('( ([target].',QUOTENAME(isnull(DestinationName,SourceName)),'is null AND [source].',quotename(SourceName),'is null) OR ([target].',QUOTENAME(isnull(DestinationName,SourceName)),' = [source].',quotename(SourceName),') ) AND
')
	from dbo.DataAttributes 
	where DataAttributes.DataObject_id = @dataObjectId and IgnoreCompare = 0 and (case @compareMode when 2 then IsUnique when 1 then 1 else null end) = 1;

	if len(@mergeOn) = 0 return null;
	SET @mergeOn = LEFT(@mergeOn, len(@mergeOn)-5);	

	/* MERGE QUERY */

	DECLARE @compareFields nvarchar(max) = '';
	SELECT 
		@compareFields += concat('([target].',QUOTENAME(isnull(DestinationName,SourceName)),'<>[source].',quotename(SourceName),' OR (([target].',QUOTENAME(isnull(DestinationName,SourceName)),' is not null and [source].',quotename(SourceName),' is null) OR ([target].',QUOTENAME(isnull(DestinationName,SourceName)),' is null and [source].',quotename(SourceName),' is not null))) OR
')
	from dbo.DataAttributes 
	where DataAttributes.DataObject_id = @dataObjectId and IgnoreCompare = 0 and (case @compareMode when 2 then iif(IsUnique=1,0,1) when 1 then 1 else null end) = 1;
	if len(@compareFields)=0 return null; else set @compareFields = LEFT(@compareFields, len(@compareFields)-4);

	declare @sourceDefinition nvarchar(max) = '';
	if @useAction = 1
		select @sourceDefinition = isnull([sourceDefinition],'') from dbo.DataObject where DataObject.id = @dataObjectId;

	DECLARE @sql nvarchar(max) = '';

	IF len(@sourceDefinition) > 0 
		SET @sql += @sourceDefinition;

	IF len(@srcFieldsUnique) = 0
		SET @sql += '
;WITH [source] AS (
	SELECT '+@srcFields+'
	FROM '+@srcDOName+'
	WHERE '+@srcFieldsTimestampFilter+'
)
,';
	ELSE
		SET @sql += '
;WITH [source_raw] AS (
	SELECT 
'+@srcFields+'
,ROW_NUMBER() OVER (PARTITION BY ' + @srcFieldsUnique + ' ' + iif(len(@srcFieldsTimestamp)=0,'ORDER BY ' + @srcFieldsUnique,'ORDER BY ' + @srcFieldsTimestamp) + ') AS N 
	FROM '+@srcDOName+'
	WHERE '+@srcFieldsTimestampFilter+'
)
,[source] AS (
	SELECT '+@srcFields+'
	FROM [source_raw]
	WHERE [N]=1
)
,';
	SET @sql += '
[target] AS(
	SELECT '+@dstFields+' 
	FROM '+@dstDOName+'
	WHERE '+@dstFieldsTimestampFilter+'
)
';

	SET @sql = '
DECLARE @res TABLE([Action] varchar(10)' + iif(@logChanges = 1, ',' + @resultNewFields + ',' + @resultOldFields, '') +'
);
declare @inserted table(details xml);
declare @deleted table(details xml);
declare @updated table(details xml);
' + @sql + '
MERGE [target] USING [source]
ON ' + @mergeOn + '  
'
-- смысл режимов @compareMode в том, что решить:
-- оставлять неактуальные строки, и только вставлять новые,
-- или обновлять поля в неактуальных строках без вставки новых
+ case 
	when @mergeUpdate = 1 
	then 
		case when @compareMode = 1
		then ''
		when @compareMode = 2 
		then 'WHEN MATCHED and ('+@compareFields+') THEN UPDATE SET'+@updateFields
		else '' end
	else '' end +'
' + iif(@mergeDelete = 1, 'WHEN NOT MATCHED BY SOURCE THEN DELETE','') + '
' + iif(@mergeInsert = 1, 'WHEN NOT MATCHED BY TARGET THEN INSERT ('+@dstFields+') VALUES ('+@srcFields+')','')+'
' + 'OUTPUT $action' + iif(@logChanges = 1, ',' + @insertedFields + ',' + @deletedFields,'') + ' INTO @res;


insert into @inserted select cast ((select ' + @resultNewFieldsSelect + ' for xml path(''INSERT'')) as xml) from @res a where [Action] =''INSERT''
insert into @deleted select cast ((select ' + @resultOldFieldsSelect + ' for xml path(''DELETE'')) as xml) from @res a where [Action] =''DELETE''
insert into @updated
select 
	cast((select 
		cast((select ' + @resultNewFieldsSelect + ' for xml path(''INSERT'')) as xml)
		,cast((select ' + @resultOldFieldsSelect + ' for xml path(''DELETE'')) as xml) 
	for xml path(''UPDATE'')
)as xml)
from @res a where [Action] =''UPDATE'';

delete from @res;

';
	return @sql;

end
GO


/*
@dataObjectId - id объекта данных из таблицы DataObject
@StartDateTime datetime - начало периода запроса данных (для фильтра по полям с меткой IsTimestamp из таблицы DataAttributes)
@EndDateTime datetime - конец периода запроса данных (для фильтра по полям с меткой IsTimestamp из таблицы DataAttributes)
@useAction bit - использовать запрос из колонки action таблицы DataObject вместо генерации запроса из метаданных
*/
-- [dbo].[usp_runIntegration] @dataObjectId = 2, @useAction = 1;
ALTER procedure [dbo].[usp_runIntegration]
	@dataObjectId int,
	@StartDateTime datetime = null,
	@EndDateTime datetime = null,
	@useAction bit = 0,
	@verbose bit = 0
as begin
	set nocount on;

	BEGIN TRY

		--if @EndDateTime is null set @EndDateTime = cast(format( getdate(),'yyyy-MM-dd HH:00') as datetime);
		--if @StartDatetime is null set @StartDatetime = dateadd(DAY, -2, @EndDateTime);		

		DECLARE @me nvarchar(max) = concat(OBJECT_SCHEMA_NAME(@@PROCID),'.',OBJECT_NAME(@@PROCID));
		declare @logCommand nvarchar(max) = concat(@me,' ',@dataobjectid,',''',format(@StartDatetime,'yyyy-MM-dd HH:mm:ss'),''',''',format(@EndDateTime,'yyyy-MM-dd HH:mm:ss'),''',',@useAction);
		declare @logMessage nvarchar(max) = '';

		if not exists (select id from dbo.DataObject where id = @dataObjectId) begin
			RAISERROR ('DataObject ID error',16,1);
		end;

		declare @logChanges bit;
		set @logChanges = (select [logChanges] from dbo.DataObject where id = @dataObjectId);

		declare @sql nvarchar(max) = 'set nocount on;';
	
		set @sql += '
DECLARE @startDateTime datetime = @StartDatetime_in, @endDateTime datetime = @EndDateTime_in;
if @EndDateTime_in is null set @EndDateTime = cast(format( getdate(),''yyyy-MM-dd HH:00'') as datetime);
if @StartDatetime_in is null set @StartDatetime = dateadd(DAY, -2, @EndDateTime);
';
	
		select @sql += dbo.usp_generate(@dataObjectId,@useAction);

		if @sql is null
			RAISERROR ('SQL generation error',16,1);

		declare @params nvarchar(max) = '';
		declare @countInsert int = 0, @countUpdate int = 0, @countDelete int = 0;
	
		set @sql +='

declare
	@mergeInsert int,
	@mergeUpdate int,
	@mergeDelete int;

SELECT @mergeInsert=count(*) FROM @res WHERE [Action]=''INSERT''; 
SELECT @mergeUpdate=count(*) FROM @res WHERE [Action]=''UPDATE'';
SELECT @mergeDELETE=count(*) FROM @res WHERE [Action]=''DELETE'';

declare @details_in nvarchar(max);
';

		if @logChanges = 0 begin	

		set @sql += 'set @details_in = (
	select a from (
		select
			@mergeInsert as [Inserted]
			,@mergeUpdate as [Updated]
			,@mergeDelete as [Deleted] 
		for xml raw
	)t(a)
);
declare @log_id bigint;
insert into dbo.[log](dataobject_id, command,IsError) select @dataobject_id_in,@logCommand_in,0;
set @log_id = SCOPE_IDENTITY();
insert into dbo.LogDetails(log_id,Details) select @log_id, details from @inserted;
insert into dbo.LogDetails(log_id,Details) select @log_id, details from @deleted;
insert into dbo.LogDetails(log_id,Details) select @log_id, details from @updated;
';

		end
		else begin

		set @sql += 'set @details_in = (
	select a from (
		select 
			@mergeInsert as [Inserted]
			,@mergeUpdate as [Updated]
			,@mergeDelete as [Deleted] 
			,cast((select * from @res for xml path) as xml) as [Details]
		for xml raw
	)t(a)
);
declare @log_id bigint;
insert into dbo.[log](dataobject_id, command,IsError) select @dataobject_id_in,@logCommand_in,0;
set @log_id = SCOPE_IDENTITY();
insert into dbo.LogDetails(log_id,Details) select @log_id, details from @inserted;
insert into dbo.LogDetails(log_id,Details) select @log_id, details from @deleted;
insert into dbo.LogDetails(log_id,Details) select @log_id, details from @updated;
';

		end
			if (@verbose = 1) begin 
				select concat('DECLARE 
@dataObject_id_in int = ',@dataObjectId,',
@logCommand_in nvarchar(max) = ''',replace(@logCommand,'''',''''''),''',
@startDateTime_in datetime = ',iif(@StartDateTime is null, 'NULL', '{ts ''' + format(@startDatetime,'yyyy-MM-dd HH:mm:ss') + '''}'), ',
@endDateTime_in datetime = ',iif(@endDateTime is null, 'NULL', '{ts ''' + format(@endDateTime,'yyyy-MM-dd HH:mm:ss') + '''}'),';
', @sql);
			end
			exec sp_executesql @sql, N'@dataObject_id_in int,@logCommand_in nvarchar(max),@startDateTime_in datetime,@endDateTime_in datetime', @logCommand_in = @logCommand, @dataObject_id_in = @dataObjectId, @StartDateTime_in = @StartDateTime, @EndDateTime_in=@EndDateTime;
	END TRY
	BEGIN CATCH
		declare @details_in nvarchar(max);
		set @details_in = (
		select a from (
			select 
				ERROR_LINE() as [ErrorLine]
				,ERROR_MESSAGE() as [ErrorMessage]
				,ERROR_NUMBER() as [ErrorNumber]
				,ERROR_PROCEDURE() as [ErrorProcedure]
				,ERROR_SEVERITY() as [ErrorSeverity]
				,ERROR_STATE() as [ErrorState]
				,sqlText.[text]
			from sys.sysprocesses 
			outer apply sys.dm_exec_sql_text([sql_handle]) as sqlText
			where spid = @@SPID
			for xml path
		)t(a)
		);
		declare @log_id bigint; 
		insert into dbo.[log](dataobject_id, command,IsError) select @dataObjectId,@logCommand,1;
		set @log_id = SCOPE_IDENTITY();
		insert into dbo.LogDetails(log_id,Details) select @log_id, @details_in;
	END CATCH
end

GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Короткое название объекта интеграции' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'name'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Описание' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'description'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Произвольный скрипт запроса данных из источника' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'SourceDefinition'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Имя объекта-источника интеграции (таблица, представление)' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'SourceName'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Имя объекта-приемника интеграции (таблица, представление)' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'DestinationName'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'1 - сравнивать по всем атрибутам; 2 - только по ключевым' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'CompareMode'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'(не) вставлять новые записи' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'MergeInsert'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'(не) обновлять записи. имеет смысл при CompareMode = 2' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'MergeUpdate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Удалять записи, отсутствующие в источнике' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'MergeDelete'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Детальный лог изменений' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'DataObject', @level2type=N'COLUMN',@level2name=N'LogChanges'
GO
USE [master]
GO
