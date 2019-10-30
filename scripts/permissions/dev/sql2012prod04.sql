USE [Hydrotel]

GO

IF NOT EXISTS (some condition im about to implement)
BEGIN

	GRANT SELECT ON [dbo].[Objects] TO [svc-hydrotelexp-dev]
	GRANT SELECT ON [dbo].[Points] TO [svc-hydrotelexp-dev]
	GRANT SELECT ON [dbo].[Samples] TO [svc-hydrotelexp-dev]
	GRANT SELECT ON [dbo].[ObjectVariants] TO [svc-hydrotelexp-dev]
	GRANT SELECT ON [dbo].[Sites] TO [svc-hydrotelexp-dev]

		print 'Login permissions executed'
END;
else
BEGIN
	print 'Login permissions already exists. did not execute'
End;
Go
