package practice.lab2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext


  /*
  case class LicenseAgreementVersion (
	AgreementVersionID: Integer,
	RevenueSubID: Integer,
	AgreementId: String,
	AgreementVersionNumber: Integer,
	AgreementStatusID: Integer,
	AgreementEffectiveDate: String,
	AgreementEndDate: String,
	MasterAgreementNumber: String,
	MasterAgreementEffectiveDate: String,
	MasterAgreementEndDate: String,
	SalesContactName: String,
	InitialLoadDate: String,
	LastLoadDate: String,
	LoadStatusCode: String,
	ProgramID: Integer,
	LicenseAgreementContractTypeID: Integer,
	CustomerGroupingID: Integer,
	PrimaryPriorAgreementId: String,
	BusinessAgreementNumber: String,
	AmendmentTypeFlag: String,
	PricingAgreementTypeID: Integer,
	PriceListCustomerTypeID: Integer,
	ServicesSalesManager: String,
	ServicesProfitCenter: String,
	PrimaryRenewedINToAgreementId: String,
	RenewalTypeId: Integer,
	AgreementConsolidationID: Integer,
	RenewalStartDate: String,
	RenewalEndDate: String,
	RenewalFlag: Integer,
	EnrollmentSubsidiaryId: Integer,
	PrimaryExpirationDate: String,
	LastLIRUpdateDate: String,
	AgreementExpectedEndDate: String,
	CALEquivalencyStatusID: Integer,
	CALEquivalencyQualifierID: Integer,
	CALEquivalencyQuantity: Integer,
	CALEquivalencyDate: String,
	PrimarySubsidiaryID: Integer,
	PrimarySubsegmentID: Integer,
	Term2EndDate: String,
	Term3StartDate: String
 )
case class LicenseCustomer (
	LicenseCustomerID: Integer,
	ProgramID: Integer,
	CustomerID: String,
	CountryId: Integer,
	CustomerName: String,
	Address1: String,
	Address2: String,
	Address3: String,
	City: String,
	StateOrProvince: String,
	PostalCode: String,
	InitialLoadDate: String,
	LastLoadDate: String,
	LoadStatusCode: String,
	TaxIdCd: String,
	PublicCustomerNumber: String,
	MIOID: String
)
case class LicenseItemDeferralDuration (
	LicenseTransactionItemID: Integer,
	DeferralDuration: Integer,
	UpdatedDate: String,
	CompensationDuration: Integer
)
case class LicensePriorAgreement (
	AgreementID: String,
	ProgramID: Integer,
	PriorAgreementID: String,
	PriorAgreementProgramID: Integer,
	LastLoadDate: String
)
case class LicenseProgram (
	ProgramID: Integer,
	ProgramCode: String,
	ProgramName: String,
	NROExceptionFlag: Integer,
	MOLPLikeFlag: Integer,
	DefaultLicenseCustomerId: Integer,
	ValidateGoldenKey: Integer,
	VolumeLicenseProgramFlag: Integer,
	DeferralDuration: Integer,
	SummaryProgramID: Integer,
	ProgramTypeID: Integer
)
case class LicenseTransactionItem (
	LicenseTransactionItemID: Integer,
	ProgramID: Integer,
	ItemID: String,
	CustomerID: Integer,
	ResellerID: Integer,
	DistributorID: Integer,
	AgreementVersionID: Integer,
	ProgramOfferingLevelID: Integer,
	ProgramOfferingTypeID: Integer,
	ProductID: Integer,
	RecurringProductID: Integer,
	UsageCountryID: Integer,
	CurrencyID: Integer,
	VolumeLicenseTypeCode: String,
	TransactionDate: String,
	ItemEntryDate: String,
	UnitQty: Integer,
	UnitPriceUSD: Float,
	UnitPriceLC: Float,
	RevenueAmt: Float,
	RevenueAmtLC: Float,
	SAPCustomerNumber: String,
	BillingCoverageStartDate: String,
	BillingCoverageEndDate: String,
	DeferredRevenueStartDate: String,
	DeferredRevenueEndDate: String,
	PrepaidIndicator: String,
	UsagePlanEffectiveDate: String,
	UsagePlanEndDate: String,
	PurchaseOrderTypeId: Integer,
	BillingTypeId: Integer,
	FirstBillAnnualPeriodFlag: Integer,
	MS_SRP: Float,
	MS_SRPCurrencyID: Integer,
	ConsumptionPeriod: String,
	MaintenanceQuarterCovered: String,
	InitialLoadDate: String,
	LastLoadDate: String,
	LoadStatusCode: String,
	PurchasePeriodID: Integer,
	OpportunityCode: String,
	AdvisorID: Integer,
	PurchaseOrderNumber: String,
	BillingOptionID: Integer,
	SystemPrice: Float,
	BillingMultiplier: Integer,
	PurchaseUnitCode: String,
	ServicesOfficeID: Integer,
	POLineItemNumber: String,
	ProgramOfferingID: Integer,
	LeadPublicCustomerID: Integer,
	AffiliatePublicCustomerID: Integer,
	OriginationTypeId: Integer,
	ProductPurchaseTypeId: Integer,
	OrderTypeId: Integer,
	NewProductFlagId: Integer,
	CommitmentPeriod: Integer,
	CommittedQuantityCnt: Integer,
	RevenueCommitment: Float,
	EstimatedFlag: Integer,
	CancelledFlag: Integer,
	LastLIRUpdateDate: String,
	POLICreatedDate: String,
	PurchaseOrderCreatedDate: String,
	EnrollmentTermID: Integer,
	FutureContractFlag: Integer,
	AlternateCommitmentDate: String,
	ReservationPOFlag: Integer,
	BillingScenarioID: Integer,
	BillingAlignmentID: Integer,
	PurchaseAlignmentID: Integer,
	BillingBlockID: Integer,
	BillingFrequencyID: Integer,
	DiscountAmount: Float,
	RevenueCommitmentLC: Float,
	BillingPlanNumber: String,
	RevenueTransactionTypeCode: String,
	BillOnDate: String,
	POLineItemKey: Integer
)
case class OrganizationTxlatLicensing (
	TxlatOrganizationId: Integer,
	SourceInstallationId: Integer,
	SourceOrganizationId: Integer,
	ReportedOrganizationId: String,
	OrgTypeChar: String,
	ProgramCode: String,
	CustomerSource: String,
	UsageCountryCode: String,
	NativeCustomerCode: String
)
// SubscriptionSource shuld be bigint??
case class SalesSubscriptionMultiple (
	SalesSubscriptionMultipleID: Integer,
	LicenseTransactionItemID: Integer,
	SubscriptionID: Integer,
	SubscriptionSource: String,
	ModifiedDate: String
)
case class GregorianMonthDate (
	CalendarDate: String,
	GregorianMonthId: Integer,
	GregorianMonthName: String,
	GregorianMonthFullName: String,
	GregorianQuarterId: Integer,
	GregorianSemesterId: Integer,
	GregorianYearID: Integer
)
case class LicenseEAType (
	EATypeID: Integer,
	PurchaseOrderTypeID: Integer,
	BillingTypeID: Integer
)
case class Program (
	ProgramId: Integer,
	ProgramCode: String,
	ProgramName: String,
	NROExceptionFlag: Integer,
	SummaryProgramId: Integer
)
case class SalesDate (
	SalesDateId: Integer,
	FiscalWeekID: Integer,
	FiscalMonthID: Integer,
	FiscalQuarterID: Integer,
	FiscalYearID: Integer,
	FiscalQuarterName: String,
	FiscalMonthName: String,
	FiscalYearName: String,
	CalendarMonthName: String,
	FWBeginDate: String,
	FWEndDate: String,
	CalendarDate: String,
	CalendarDateName: String,
	MonthRelativeID: Integer,
	FMBeginDate: String,
	FMEndDate: String,
	TransactionDataPopulated: Integer,
	FiscalSemesterID: Integer,
	FiscalSemesterName: String,
	ReportingWeekId: Integer,
	RWBeginDate: String,
	RWEndDate: String,
	GregorianMonthID: Integer,
	GregorianMonthBeginDate: String,
	GregorianMonthEndDate: String,
	GregorianMonthName: String,
	GregorianMonthFullName: String,
	GregorianQuarterID: Integer,
	GregorianQuarterName: String,
	GregorianSemesterID: Integer,
	GregorianSemesterName: String,
	GregorianYearID: Integer,
	GregorianYearName: String,
	AggregatedGregorianSalesDateID: Integer
)*/
	object Jeshu_Assignment {
    
   def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Emergency 911 Analysis").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    

  }
  
  
}