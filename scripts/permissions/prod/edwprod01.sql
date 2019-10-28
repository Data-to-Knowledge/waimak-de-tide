USE [Hydro]

GO

IF NOT EXISTS (some condition im about to implement)
BEGIN

	GRANT SELECT ON [dbo].[TSDataNumericDaily] TO [svc-hydrotelexp-dev]
	GRANT SELECT ON [dbo].[TSDataNumericDailySumm] TO [svc-hydrotelexp-dev]
	GRANT SELECT ON [dbo].[vDatasetTypeNamesActive] TO [svc-hydrotelexp-dev]
	GRANT SELECT ON [dbo].[ExternalSite] TO [svc-hydrotelexp-dev]

		print 'Login permissions executed'
END;
else
BEGIN
	print 'Login permissions already exists. did not execute'
End;
Go

USE [ConsentsReporting]

GO

IF NOT EXISTS (some condition im about to implement)
BEGIN

	GRANT SELECT ON [reporting].[CrcAlloSiteSumm] TO [svc-hydrotelexp-dev]

		print 'Login permissions executed'
END;
else
BEGIN
	print 'Login permissions already exists. did not execute'
End;
Go
