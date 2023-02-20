<?php
namespace Master\Import\Model\Import\CustomImport;
 
interface RowValidatorInterface extends \Magento\Framework\Validator\ValidatorInterface
{
       const ERROR_INVALID_TITLE= 'InvalidValueTITLE';
       const ERROR_TITLE_IS_EMPTY = 'EmptyMessage';
       public function init($context);
}