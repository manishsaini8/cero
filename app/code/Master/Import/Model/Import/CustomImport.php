<?php 
namespace Master\Import\Model\Import; 
use Master\Import\Model\Import\CustomImport\RowValidatorInterface as ValidatorInterface; 
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregatorInterface; 
use Magento\Framework\App\ResourceConnection; 

class CustomImport extends \Magento\ImportExport\Model\Import\Entity\AbstractEntity 
{ 
     const ID = 'id'; const unique_part_id = 'unique_part_id';  const  part_name = 'part_name';
     const make = 'make'; const model = 'model';  const  vehicle_type = 'vehicle_type';
	const  part_category = 'part_category'; const size = 'size'; const position_1 = 'position_1'; const position_2 = 'position_2';
	 const height_cm = 'height_cm'; const length_cm = 'length_cm'; const width_cm = 'width_cm';
	 const weight_kg = 'weight_kg'; const hsn_code = 'hsn_code'; const description = 'description';
	 const shipping_information = 'shipping_information'; const TABLE_Entity = 'master_data'; /** * Validation failure message template definitions * * @var array */ protected $_messageTemplates = [ ValidatorInterface::ERROR_TITLE_IS_EMPTY => 'Name is empty',];
 
     protected $_permanentAttributes = [self::ID];
     protected $needColumnCheck = true;
     protected $groupFactory;
     protected $validColumnNames = [self::ID, self::unique_part_id,
	 self::part_name, self::make,
	 self::model, self::vehicle_type, self::part_category,
	 self::size, self::position_1,
	 self::position_2, self::height_cm,
	 self::length_cm, self::width_cm,
	 self::weight_kg, self::hsn_code,
	 self::description, self::shipping_information,];
     protected $logInHistory = true;
     protected $_validators = [];
     protected $_connection;
     protected $_resource;
    
     public function __construct(
     \Magento\Framework\Json\Helper\Data $jsonHelper,
     \Magento\ImportExport\Helper\Data $importExportData,
     \Magento\ImportExport\Model\ResourceModel\Import\Data $importData,
     \Magento\Framework\App\ResourceConnection $resource,
     \Magento\ImportExport\Model\ResourceModel\Helper $resourceHelper,
     \Magento\Framework\Stdlib\StringUtils $string,
     ProcessingErrorAggregatorInterface $errorAggregator,
     \Magento\Customer\Model\GroupFactory $groupFactory)
     {
          $this->jsonHelper = $jsonHelper;
    	  $this->_importExportData = $importExportData;
    	  $this->_resourceHelper = $resourceHelper;
    	  $this->_dataSourceModel = $importData;
    	  $this->_resource = $resource;
    	  $this->_connection = $resource->getConnection(\Magento\Framework\App\ResourceConnection::DEFAULT_CONNECTION);
    	  $this->errorAggregator = $errorAggregator;
    	  $this->groupFactory = $groupFactory;
     }
 
     public function getValidColumnNames()
     {
          return $this->validColumnNames;
     }
 
     public function getEntityTypeCode()
     {
          return 'master_data';
     }
 
     public function validateRow(array $rowData, $rowNum)
     {
          if (isset($this->_validatedRows[$rowNum]))
          {
       	       return !$this->getErrorAggregator()->isRowInvalid($rowNum);
          }
          $this->_validatedRows[$rowNum] = true;
          return !$this->getErrorAggregator()->isRowInvalid($rowNum);
     }

     protected function _importData()
     {
          $this->saveEntity();
          return true;
     }
    
     public function saveEntity()
     {
          $this->saveAndReplaceEntity();
          return $this;
     }
 
     public function replaceEntity()
     {
          $this->saveAndReplaceEntity();
          return $this;
     }
    
     public function deleteEntity()
     {
          $listTitle = [];
          while ($bunch = $this->_dataSourceModel->getNextBunch())
          {
               foreach ($bunch as $rowNum => $rowData)
               {
                    $this->validateRow($rowData, $rowNum);
                    if (!$this->getErrorAggregator()->isRowInvalid($rowNum))
	                   {
                         $rowTtile = $rowData[self::ID];
                         $listTitle[] = $rowTtile;
                    }
                    if ($this->getErrorAggregator()->hasToBeTerminated())
 	                   {
                         $this->getErrorAggregator()->addRowToSkip($rowNum);
                    }
               }
          }
          if ($listTitle)
          {
               $this->deleteEntityFinish(array_unique($listTitle),self::TABLE_Entity);
          }
          return $this;
     }
     
     protected function saveAndReplaceEntity()
     {
          $behavior = $this->getBehavior();
          $listTitle = [];
          while ($bunch = $this->_dataSourceModel->getNextBunch())
          {
               $entityList = [];
               foreach ($bunch as $rowNum => $rowData)
	              {
                    if (!$this->validateRow($rowData, $rowNum))
	                   {
                         $this->addRowError(ValidatorInterface::ERROR_TITLE_IS_EMPTY, $rowNum);
                         continue;
                    }
                    if ($this->getErrorAggregator()->hasToBeTerminated())
	                   {
                         $this->getErrorAggregator()->addRowToSkip($rowNum);
                         continue;
                    }
                    $rowTtile= $rowData[self::ID];
                    $listTitle[] = $rowTtile;
                    $entityList[$rowTtile][] = [
                    self::ID => $rowData[self::ID],
                    self::unique_part_id => $rowData[self::unique_part_id],
                    self::part_name => $rowData[self::part_name],
                    self::make => $rowData[self::make],
                    self::model => $rowData[self::model],
                    self::vehicle_type => $rowData[self::vehicle_type],
                    self::part_category => $rowData[self::part_category],
                    self::size => $rowData[self::size],
                    self::position_1 => $rowData[self::position_1],
                    self::position_2 => $rowData[self::position_2],
                    self::height_cm => $rowData[self::height_cm],
                    self::length_cm => $rowData[self::length_cm],
                    self::width_cm => $rowData[self::width_cm],
                    self::weight_kg => $rowData[self::weight_kg],
                    self::hsn_code => $rowData[self::hsn_code],
                    self::description => $rowData[self::description],
                    self::shipping_information => $rowData[self::shipping_information],];
               }
               if (\Magento\ImportExport\Model\Import::BEHAVIOR_REPLACE == $behavior)
	              {
                    if ($listTitle)
	                   {
                         if ($this->deleteEntityFinish(array_unique(  $listTitle), self::TABLE_Entity))
		                        {
                              $this->saveEntityFinish($entityList, self::TABLE_Entity);
                         }
                    }
               }
	              elseif (\Magento\ImportExport\Model\Import::BEHAVIOR_APPEND == $behavior)
	              {
                    $this->saveEntityFinish($entityList, self::TABLE_Entity);
               }
          }
          return $this;
     }
 
     protected function saveEntityFinish(array $entityData, $table)
     {
          if ($entityData)
          {
               $tableName = $this->_connection->getTableName($table);
               $entityIn = [];
               foreach ($entityData as $id => $entityRows)
	              {
                    foreach ($entityRows as $row)
		                   {
                         $entityIn[] = $row;
                    }
               }
               if ($entityIn)
	              {
                    $this->_connection->insertOnDuplicate($tableName, $entityIn,[
                     self::ID,
                     self::unique_part_id,
					 self::part_name, self::make,
					 self::model, self::vehicle_type,
					 self::part_category,
					 self::size, self::position_1,
					 self::position_2, self::height_cm,
					 self::length_cm, self::width_cm,
					 self::weight_kg, self::hsn_code,
					 self::description, self::shipping_information]);
                 }
          }
          return $this;
     }
 
     protected function deleteEntityFinish(array $ids, $table)
     {
          if ($table && $listTitle)
          {
               try
	              {
                    $this->countItemsDeleted += $this->_connection->delete(
                    $this->_connection->getTableName($table),
                    $this->_connection->quoteInto('id IN (?)', $ids));
                    return true;
               }
	              catch (\Exception $e)
	              {
                    return false;
               }
          } 
          else
          {
               return false;
          }
     }
}