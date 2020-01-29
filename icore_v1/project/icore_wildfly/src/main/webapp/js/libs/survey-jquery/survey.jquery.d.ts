/*Type definitions for Survey JavaScript library v1.0.6
Project: http://surveyjs.io/
Definitions by: Devsoft Baltic OÜ <https://github.com/surveyjs/>
*/

import "./chunks/localization";
import "../main.scss";

import "../../main.scss";
export let Version: string;

export var __assign: any;
export function __extends(thisClass: any, baseClass: any): void;
export var __decorate: (decorators: any, target: any, key: any, desc: any) => any;

export declare var surveyCss: {
    currentType: string;
    getCss: () => any;
};
export declare var defaultStandardCss: {
    root: string;
    header: string;
    body: string;
    footer: string;
    navigationButton: string;
    navigation: {
        complete: string;
        prev: string;
        next: string;
        start: string;
    };
    progress: string;
    progressBar: string;
    page: {
        root: string;
        title: string;
        description: string;
    };
    pageTitle: string;
    pageDescription: string;
    row: string;
    question: {
        mainRoot: string;
        title: string;
        description: string;
        comment: string;
        required: string;
        titleRequired: string;
        indent: number;
        footer: string;
    };
    panel: {
        title: string;
        description: string;
        container: string;
    };
    error: {
        root: string;
        icon: string;
        item: string;
    };
    boolean: {
        root: string;
        item: string;
    };
    checkbox: {
        root: string;
        item: string;
        label: string;
        itemControl: string;
        controlLabel: string;
        other: string;
    };
    comment: string;
    dropdown: {
        root: string;
        control: string;
        selectWrapper: string;
        other: string;
    };
    matrix: {
        root: string;
        label: string;
    };
    matrixdropdown: {
        root: string;
    };
    matrixdynamic: {
        root: string;
        button: string;
    };
    paneldynamic: {
        root: string;
        title: string;
        button: string;
    };
    multipletext: {
        root: string;
        itemTitle: string;
        row: string;
        itemValue: string;
    };
    radiogroup: {
        root: string;
        item: string;
        label: string;
        itemControl: string;
        controlLabel: string;
        other: string;
    };
    rating: {
        root: string;
        item: string;
        selected: string;
        minText: string;
        itemText: string;
        maxText: string;
    };
    text: string;
    expression: string;
    file: {
        placeholderInput: string;
    };
    saveData: {
        root: string;
        saving: string;
        error: string;
        success: string;
        saveAgainButton: string;
    };
    window: {
        root: string;
        body: string;
        header: {
            root: string;
            title: string;
            button: string;
            buttonExpanded: string;
            buttonCollapsed: string;
        };
    };
};

export declare var defaultBootstrapCss: {
    root: string;
    header: string;
    body: string;
    footer: string;
    navigationButton: string;
    navigation: {
        complete: string;
        prev: string;
        next: string;
        start: string;
    };
    progress: string;
    progressBar: string;
    page: {
        root: string;
        title: string;
        description: string;
    };
    pageTitle: string;
    pageDescription: string;
    row: string;
    question: {
        mainRoot: string;
        title: string;
        description: string;
        comment: string;
        required: string;
        titleRequired: string;
        indent: number;
    };
    panel: {
        title: string;
        description: string;
        container: string;
    };
    error: {
        root: string;
        icon: string;
        item: string;
    };
    boolean: {
        root: string;
        item: string;
    };
    checkbox: {
        root: string;
        item: string;
        itemControl: string;
        controlLabel: string;
        other: string;
    };
    comment: string;
    dropdown: {
        root: string;
        control: string;
        other: string;
    };
    matrix: {
        root: string;
        label: string;
    };
    matrixdropdown: {
        root: string;
    };
    matrixdynamic: {
        root: string;
        button: string;
    };
    paneldynamic: {
        root: string;
        button: string;
    };
    multipletext: {
        root: string;
        itemTitle: string;
        itemValue: string;
    };
    radiogroup: {
        root: string;
        item: string;
        label: string;
        itemControl: string;
        controlLabel: string;
        other: string;
    };
    rating: {
        root: string;
        item: string;
        selected: string;
        minText: string;
        itemText: string;
        maxText: string;
    };
    text: string;
    expression: string;
    file: {
        placeholderInput: string;
    };
    saveData: {
        root: string;
        saving: string;
        error: string;
        success: string;
        saveAgainButton: string;
    };
    window: {
        root: string;
        body: string;
        header: {
            root: string;
            title: string;
            button: string;
            buttonExpanded: string;
            buttonCollapsed: string;
        };
    };
};

export declare var defaultBootstrapMaterialCss: {
    root: string;
    header: string;
    body: string;
    footer: string;
    navigationButton: string;
    navigation: {
        complete: string;
        prev: string;
        next: string;
        start: string;
    };
    progress: string;
    progressBar: string;
    page: {
        root: string;
        title: string;
        description: string;
    };
    pageTitle: string;
    pageDescription: string;
    row: string;
    question: {
        mainRoot: string;
        title: string;
        description: string;
        comment: string;
        required: string;
        titleRequired: string;
        indent: number;
    };
    panel: {
        title: string;
        description: string;
        container: string;
    };
    error: {
        root: string;
        icon: string;
        item: string;
    };
    boolean: {
        root: string;
        item: string;
    };
    checkbox: {
        root: string;
        item: string;
        itemControl: string;
        controlLabel: string;
        other: string;
    };
    comment: string;
    dropdown: {
        root: string;
        control: string;
        other: string;
    };
    matrix: {
        root: string;
        row: string;
        label: string;
        itemValue: string;
    };
    matrixdropdown: {
        root: string;
        itemValue: string;
    };
    matrixdynamic: {
        root: string;
        button: string;
        itemValue: string;
    };
    paneldynamic: {
        root: string;
        button: string;
    };
    multipletext: {
        root: string;
        itemTitle: string;
        row: string;
        itemValue: string;
    };
    radiogroup: {
        root: string;
        item: string;
        label: string;
        itemControl: string;
        controlLabel: string;
        other: string;
    };
    rating: {
        root: string;
        item: string;
        selected: string;
        minText: string;
        itemText: string;
        maxText: string;
    };
    text: string;
    expression: string;
    file: {
        root: string;
        placeholderInput: string;
    };
    saveData: {
        root: string;
        saving: string;
        error: string;
        success: string;
        saveAgainButton: string;
    };
    window: {
        root: string;
        body: string;
        header: {
            root: string;
            title: string;
            button: string;
            buttonExpanded: string;
            buttonCollapsed: string;
        };
    };
};

export declare class Survey extends SurveyModel {
    static cssType: string;
    onRendered: Event<(sender: SurveyModel) => any, any>;
    koCurrentPage: any;
    koIsFirstPage: any;
    koIsLastPage: any;
    koIsNavigationButtonsShowing: any;
    dummyObservable: any;
    koState: any;
    koProgress: any;
    koProgressText: any;
    koAfterRenderPage: any;
    koCompletedState: any;
    koCompletedStateText: any;
    koCompletedStateCss: any;
    koTimerInfoText: any;
    constructor(jsonObj?: any, renderedElement?: any, css?: any);
    readonly cssNavigationComplete: string;
    readonly cssNavigationPrev: string;
    readonly cssNavigationStart: string;
    readonly cssNavigationNext: string;
    css: any;
    render(element?: any): void;
    koEventAfterRender(element: any, survey: any): void;
    loadSurveyFromService(surveyId?: string, clientId?: string, renderedElement?: any): void;
    protected setCompleted(): void;
    start(): void;
    protected createNewPage(name: string): Page;
    protected getHtmlTemplate(): string;
    protected onBeforeCreating(): void;
    protected currentPageChanged(newValue: PageModel, oldValue: PageModel): void;
    pageVisibilityChanged(page: IPage, newValue: boolean): void;
    protected onLoadSurveyFromService(): void;
    protected onLoadingSurveyFromService(): void;
    protected setCompletedState(value: string, text: string): void;
    protected onTimeSpentChanged(): void;
}

export declare class QuestionRow extends QuestionRowModel {
    panel: PanelModelBase;
    koVisible: any;
    koElements: any;
    koGetType: any;
    koElementAfterRender: any;
    constructor(panel: PanelModelBase);
    addElement(q: IElement): void;
    protected onVisibleChanged(): void;
    getElementType(el: any): "survey-panel" | "survey-question";
    koAfterRender(el: any, con: any): void;
}
export declare class PanelImplementorBase {
    panel: PanelModelBase;
    koRows: any;
    constructor(panel: PanelModelBase);
}
export declare class Panel extends PanelModel {
    koVisible: any;
    koInnerMargin: any;
    koRenderWidth: any;
    koElementType: any;
    koErrors: any;
    koCss: any;
    koIsExpanded: any;
    koIsCollapsed: any;
    doExpand: any;
    constructor(name?: string);
    protected createRow(): QuestionRowModel;
    protected onCreating(): void;
    protected onNumChanged(value: number): void;
    protected onRenderWidthChanged(): void;
    endLoadingFromJson(): void;
    protected onVisibleChanged(): void;
}
export declare class Page extends PageModel {
    constructor(name?: string);
    protected createRow(): QuestionRowModel;
    protected createNewPanel(name: string): PanelModel;
    protected onCreating(): void;
    protected onNumChanged(value: number): void;
}

export declare class QuestionImplementorBase {
    question: QuestionBase;
    koVisible: any;
    koErrors: any;
    koPaddingLeft: any;
    koPaddingRight: any;
    koRenderWidth: any;
    koTemplateName: any;
    koElementType: any;
    constructor(question: QuestionBase);
    protected updateQuestion(): void;
    protected onSurveyLoad(): void;
    protected onVisibilityChanged(): void;
    protected onRenderWidthChanged(): void;
}

export declare class QuestionImplementor extends QuestionImplementorBase {
    question: Question;
    koValue: any;
    koComment: any;
    koIsReadOnly: any;
    constructor(question: Question);
    protected updateQuestion(): void;
    protected onValueChanged(): void;
    protected onCommentChanged(): void;
    protected onVisibleIndexChanged(): void;
    protected onSurveyLoad(): void;
    protected onReadOnlyChanged(): void;
    protected onErrorsChanged(): void;
    protected createkoValue(): any;
    protected setkoValue(newValue: any): void;
    protected updateValue(newValue: any): void;
    protected updateComment(newValue: any): void;
    protected getNo(): string;
    protected updateKoDummy(): void;
    protected koQuestionAfterRender(elements: any, con: any): void;
}

export declare class QuestionSelectBaseImplementor extends QuestionImplementor {
    koOtherVisible: any;
    koVisibleChoices: any;
    constructor(question: Question);
    protected readonly isOtherSelected: boolean;
}
export declare class QuestionCheckboxBaseImplementor extends QuestionSelectBaseImplementor {
    koWidth: any;
    constructor(question: Question);
    protected onColCountChanged(): void;
    protected readonly colWidth: string;
}

export declare class QuestionCheckbox extends QuestionCheckboxModel {
    name: string;
    constructor(name: string);
    getItemClass(item: any): string;
}

export declare class QuestionComment extends QuestionCommentModel {
    name: string;
    constructor(name: string);
}

export declare class QuestionDropdown extends QuestionDropdownModel {
    name: string;
    constructor(name: string);
}

export declare class QuestionFileImplementor extends QuestionImplementor {
    koDataUpdater: any;
    koData: any;
    koHasValue: any;
    constructor(question: Question);
}
export declare class QuestionFile extends QuestionFileModel {
    name: string;
    constructor(name: string);
}

export declare class QuestionHtml extends QuestionHtmlModel {
    name: string;
    constructor(name: string);
}

export declare class MatrixRow extends MatrixRowModel {
    fullName: string;
    koValue: any;
    constructor(item: ItemValue, fullName: string, data: IMatrixData, value: any);
    protected onValueChanged(): void;
}
export declare class QuestionMatrix extends QuestionMatrixModel {
    name: string;
    constructor(name: string);
    protected createMatrixRow(item: ItemValue, fullName: string, value: any): MatrixRowModel;
    getItemCss(row: any, column: any): string;
}

export declare class QuestionMatrixBaseImplementor extends QuestionImplementor {
    koCellAfterRender: any;
    constructor(question: Question);
}
export declare class QuestionMatrixDropdown extends QuestionMatrixDropdownModel {
    name: string;
    constructor(name: string);
}

export declare class QuestionMatrixDynamicImplementor extends QuestionMatrixBaseImplementor {
    koRows: any;
    koRecalc: any;
    koAddRowClick: any;
    koRemoveRowClick: any;
    koOverflowX: any;
    koCanAddRow: any;
    koCanRemoveRow: any;
    constructor(question: Question);
    protected onUpdateCells(): void;
    protected onColumnChanged(): void;
    protected onRowCountChanged(): void;
    protected addRow(): void;
    protected removeRow(row: MatrixDynamicRowModel): void;
}
export declare class QuestionMatrixDynamic extends QuestionMatrixDynamicModel {
    name: string;
    koCellAfterRender: any;
    constructor(name: string);
}

export declare class QuestionPanelDynamicImplementor extends QuestionImplementor {
    koPanels: any;
    koRecalc: any;
    koAddPanelClick: any;
    koRemovePanelClick: any;
    koPrevPanelClick: any;
    koNextPanelClick: any;
    koCanAddPanel: any;
    koCanRemovePanel: any;
    koPanel: any;
    koIsList: any;
    koIsProgressTop: any;
    koIsProgressBottom: any;
    koIsNextButton: any;
    koIsPrevButton: any;
    koIsRange: any;
    koRangeValue: any;
    koRangeMax: any;
    constructor(question: Question);
    protected onPanelCountChanged(): void;
    protected onRenderModeChanged(): void;
    protected onCurrentIndexChanged(): void;
    protected addPanel(): void;
    protected removePanel(val: any): void;
}
export declare class QuestionPanelDynamic extends QuestionPanelDynamicModel {
    name: string;
    constructor(name: string);
    protected createNewPanelObject(): PanelModel;
}

export declare class MultipleTextItem extends MultipleTextItemModel {
    name: any;
    koValue: any;
    constructor(name?: any, title?: string);
    protected createEditor(name: string): QuestionTextModel;
    onValueChanged(newValue: any): void;
}
export declare class QuestionMultipleTextImplementor extends QuestionImplementor {
    koRows: any;
    constructor(question: Question);
    protected onColCountChanged(): void;
}
export declare class QuestionMultipleText extends QuestionMultipleTextModel {
    name: string;
    constructor(name: string);
    protected createTextItem(name: string, title: string): MultipleTextItemModel;
}

export declare class QuestionRadiogroup extends QuestionRadiogroupModel {
    name: string;
    constructor(name: string);
    getItemClass(item: any): string;
}

export declare class QuestionRating extends QuestionRatingModel {
    name: string;
    itemCss: string;
    selectedCss: string;
    constructor(name: string);
    protected onSetData(): void;
}

export declare class QuestionTextImplementor extends QuestionImplementor {
    question: Question;
    constructor(question: Question);
    protected updateValue(newValue: any): void;
}
export declare class QuestionText extends QuestionTextModel {
    name: string;
    constructor(name: string);
}

export declare class QuestionBooleanImplementor extends QuestionImplementor {
    question: Question;
    koIndeterminate: any;
    koCheckedValue: any;
    constructor(question: Question);
    protected setkoValue(newValue: any): void;
    protected updateCheckedValue(newValue: any): void;
}
export declare class QuestionBoolean extends QuestionBooleanModel {
    name: string;
    constructor(name: string);
    getItemCss(row: any, column: any): string;
}

export declare class QuestionEmpty extends QuestionEmptyModel {
    name: string;
    constructor(name: string);
}

export declare class QuestionExpressionImplementor extends QuestionImplementor {
    question: Question;
    koDisplayValue: any;
    constructor(question: Question);
    protected onValueChanged(): void;
}
export declare class QuestionExpression extends QuestionExpressionModel {
    name: string;
    constructor(name: string);
}

export declare class SurveyWindow extends SurveyWindowModel {
    koExpanded: any;
    koExpandedCss: any;
    doExpand: any;
    constructor(jsonObj?: any, initialModel?: SurveyModel);
    protected createSurvey(jsonObj: any): SurveyModel;
    protected template: string;
    protected doShowingChanged(): void;
    protected getDefaultTemplate(): string;
    readonly css: any;
}

export declare var koTemplate: any;
export declare class SurveyTemplateText {
    constructor();
    addText(newText: string, id: string, name: string): void;
    replaceText(replaceText: string, id: string, questionType?: string): void;
    protected getId(id: string, questionType: string): string;
    protected text: string;
}

export interface HashTable<T> {
    [key: string]: T;
}
export declare class Helpers {
    /**
      * A static methods that returns true if a value underfined, null, empty string or empty array.
      * @param value
      */
    static isValueEmpty(value: any): boolean;
    static isTwoValueEquals(x: any, y: any): boolean;
    static randomizeArray<T>(array: Array<T>): Array<T>;
}

export declare class ValidatorResult {
        value: any;
        error: SurveyError;
        constructor(value: any, error?: SurveyError);
}
/**
    * Base SurveyJS validator class.
    */
export declare class SurveyValidator extends Base {
        text: string;
        constructor();
        protected getErrorText(name: string): string;
        protected getDefaultErrorText(name: string): string;
        validate(value: any, name?: string): ValidatorResult;
}
export interface IValidatorOwner {
        validators: Array<SurveyValidator>;
        validatedValue: any;
        getValidatorTitle(): string;
}
export declare class ValidatorRunner {
        run(owner: IValidatorOwner): SurveyError;
}
/**
    * Validate numeric values.
    */
export declare class NumericValidator extends SurveyValidator {
        minValue: number;
        maxValue: number;
        constructor(minValue?: number, maxValue?: number);
        getType(): string;
        validate(value: any, name?: string): ValidatorResult;
        protected getDefaultErrorText(name: string): any;
}
/**
    * Validate text values
    */
export declare class TextValidator extends SurveyValidator {
        minLength: number;
        maxLength: number;
        constructor(minLength?: number, maxLength?: number);
        getType(): string;
        validate(value: any, name?: string): ValidatorResult;
        protected getDefaultErrorText(name: string): any;
}
export declare class AnswerCountValidator extends SurveyValidator {
        minCount: number;
        maxCount: number;
        constructor(minCount?: number, maxCount?: number);
        getType(): string;
        validate(value: any, name?: string): ValidatorResult;
        protected getDefaultErrorText(name: string): string;
}
/**
    * Use it to validate the text by regular expressions.
    */
export declare class RegexValidator extends SurveyValidator {
        regex: string;
        constructor(regex?: string);
        getType(): string;
        validate(value: any, name?: string): ValidatorResult;
}
/**
    * Validate e-mail address in the text input
    */
export declare class EmailValidator extends SurveyValidator {
        constructor();
        getType(): string;
        validate(value: any, name?: string): ValidatorResult;
        protected getDefaultErrorText(name: string): any;
}

/**
  * Array of ItemValue is used in checkox, dropdown and radiogroup choices, matrix columns and rows.
  * It has two main properties: value and text. If text is empty, value is used for displaying.
  * The text property is localizable and support markdown.
  */
export declare class ItemValue {
    static Separator: string;
    static createArray(locOwner: ILocalizableOwner): Array<ItemValue>;
    static setupArray(items: Array<ItemValue>, locOwner: ILocalizableOwner): void;
    static setData(items: Array<ItemValue>, values: Array<any>): void;
    static getData(items: Array<ItemValue>): any;
    static getItemByValue(items: Array<ItemValue>, val: any): ItemValue;
    static getTextOrHtmlByValue(items: Array<ItemValue>, val: any): string;
    static NotifyArrayOnLocaleChanged(items: Array<ItemValue>): void;
    constructor(value: any, text?: string);
    getType(): string;
    readonly locText: LocalizableString;
    locOwner: ILocalizableOwner;
    value: any;
    readonly hasText: boolean;
    text: string;
    getData(): any;
    setData(value: any): void;
}

export interface ISurveyData {
        getValue(name: string): any;
        setValue(name: string, newValue: any): any;
        getComment(name: string): string;
        setComment(name: string, newValue: string): any;
        getAllValues(): any;
}
export interface ITextProcessor {
        processText(text: string, returnDisplayValue: boolean): string;
        processTextEx(text: string): any;
}
export interface ISurvey extends ITextProcessor {
        currentPage: IPage;
        pages: Array<IPage>;
        isPageStarted(page: IPage): boolean;
        pageVisibilityChanged(page: IPage, newValue: boolean): any;
        panelVisibilityChanged(panel: IPanel, newValue: boolean): any;
        questionVisibilityChanged(question: IQuestion, newValue: boolean): any;
        questionsOrder: string;
        questionAdded(question: IQuestion, index: number, parentPanel: any, rootPanel: any): any;
        panelAdded(panel: IElement, index: number, parentPanel: any, rootPanel: any): any;
        questionRemoved(question: IQuestion): any;
        panelRemoved(panel: IElement): any;
        validateQuestion(question: IQuestion): SurveyError;
        validatePanel(panel: IPanel): SurveyError;
        hasVisibleQuestionByValueName(valueName: string): boolean;
        processHtml(html: string): string;
        isDisplayMode: boolean;
        isDesignMode: boolean;
        isLoadingFromJson: boolean;
        requiredText: string;
        questionStartIndex: string;
        questionTitleLocation: string;
        questionErrorLocation: string;
        getQuestionTitleTemplate(): string;
        storeOthersAsComment: boolean;
        uploadFile(name: string, file: File, storeDataAsText: boolean, uploadingCallback: (status: string) => any): boolean;
        updateQuestionCssClasses(question: IQuestion, cssClasses: any): any;
        updatePanelCssClasses(panel: IPanel, cssClasses: any): any;
        afterRenderQuestion(question: IQuestion, htmlElement: any): any;
        afterRenderPanel(panel: IElement, htmlElement: any): any;
        afterRenderPage(htmlElement: any): any;
        matrixRowAdded(question: IQuestion): any;
        matrixRowRemoved(question: IQuestion, rowIndex: number, row: any): any;
        matrixCellCreated(question: IQuestion, options: any): any;
        matrixAfterCellRender(question: IQuestion, options: any): any;
        matrixCellValueChanged(question: IQuestion, options: any): any;
        matrixCellValidate(question: IQuestion, options: any): SurveyError;
        dynamicPanelAdded(question: IQuestion): any;
        dynamicPanelRemoved(question: IQuestion, panelIndex: number): any;
}
export interface ISurveyImpl {
        geSurveyData(): ISurveyData;
        getSurvey(): ISurvey;
        getTextProcessor(): ITextProcessor;
}
export interface IConditionRunner {
        runCondition(values: HashTable<any>): any;
}
export interface ISurveyElement {
        name: string;
        setSurveyImpl(value: ISurveyImpl): any;
        onSurveyLoad(): any;
        getType(): string;
}
export interface IElement extends IConditionRunner, ISurveyElement {
        visible: boolean;
        isVisible: boolean;
        parent: IPanel;
        renderWidth: string;
        width: string;
        rightIndent: number;
        startWithNewLine: boolean;
        isPanel: boolean;
        removeElement(el: IElement): boolean;
        onLocaleChanged(): any;
        onAnyValueChanged(name: string): any;
        updateCustomWidgets(): any;
}
export interface IQuestion extends IElement {
        hasTitle: boolean;
        isEmpty(): boolean;
        setVisibleIndex(value: number): number;
        onSurveyValueChanged(newValue: any): any;
        onReadOnlyChanged(): any;
        supportGoNextPageAutomatic(): boolean;
        clearUnusedValues(): any;
        displayValue: any;
        getValueName(): string;
        clearValue(): any;
        clearValueIfInvisible(): any;
        isAnswerCorrect(): boolean;
}
export interface IParentElement {
        addElement(element: IElement, index: number): any;
        removeElement(element: IElement): boolean;
}
export interface IPanel extends ISurveyElement, IParentElement {
        getQuestionTitleLocation(): string;
        parent: IPanel;
}
export interface IPage extends ISurveyElement, IParentElement, IConditionRunner {
}
/**
    * The base class for SurveyJS objects.
    */
export declare class Base {
        static commentPrefix: string;
        /**
            * A static methods that returns true if a value underfined, null, empty string or empty array.
            * @param value
            */
        isValueEmpty(value: any): boolean;
        protected IsPropertyEmpty(value: any): boolean;
        protected isLoadingFromJsonValue: boolean;
        onPropertyChanged: Event<(sender: Base, options: any) => any, any>;
        setPropertyValueCoreHandler: (propertiesHash: any, name: string, val: any) => void;
        constructor();
        /**
            * Returns the type of the object as a string as it represents in the json.
            */
        getType(): string;
        /**
            * Returns the element template name without prefix. Typically it equals to getType()
            * @see getType
            */
        getTemplate(): string;
        /**
            * Returns true if the object is loading from Json at the current moment.
            */
        readonly isLoadingFromJson: boolean;
        startLoadingFromJson(): void;
        endLoadingFromJson(): void;
        onLocaleChanged(): void;
        /**
            * Returns the property value by name
            * @param name property name
            */
        getPropertyValue(name: string, defaultValue?: any): any;
        protected setPropertyValueCore(propertiesHash: any, name: string, val: any): void;
        /**
            * set property value
            * @param name property name
            * @param val new property value
            */
        setPropertyValue(name: string, val: any): void;
        protected propertyValueChanged(name: string, oldValue: any, newValue: any): void;
        /**
            * Register a function that will be called on a property value changed.
            * @param name the property name
            * @param func the function with no parameters that will be called on property changed.
            * @param key an optional parameter. If there is already a registered function for this property witht the same key, it will be overwritten.
            */
        registerFunctionOnPropertyValueChanged(name: string, func: any, key?: string): void;
        /**
            * Register a function that will be called on a property value changed from the names list.
            * @param names the list of properties names
            * @param func the function with no parameters that will be called on property changed.
            * @param key an optional parameter. If there is already a registered function for this property witht the same key, it will be overwritten.
            */
        registerFunctionOnPropertiesValueChanged(names: Array<string>, func: any, key?: string): void;
        /**
            * Unregister notification on property value changed
            * @param name the property name
            * @param key the key with which you have registered the notification for this property. It can be null.
            */
        unRegisterFunctionOnPropertyValueChanged(name: string, key?: string): void;
        /**
            * Unregister notification on property value changed for all properties in the names list.
            * @param names the list of properties names
            * @param key the key with which you have registered the notification for this property. It can be null.
            */
        unRegisterFunctionOnPropertiesValueChanged(names: Array<string>, key?: string): void;
        createCustomLocalizableObj(name: string): void;
        protected createLocalizableString(name: string, owner: ILocalizableOwner, useMarkDown?: boolean): LocalizableString;
        getLocalizableString(name: string): LocalizableString;
        getLocalizableStringText(name: string, defaultStr?: string): string;
        setLocalizableStringText(name: string, value: string): void;
        protected createItemValues(name: string): Array<any>;
        protected createNewArray(name: string, onPush?: any, onRemove?: any): Array<any>;
        protected setArray(src: any[], dest: any[], isItemValues: boolean, onPush: any): void;
        protected isTwoValueEquals(x: any, y: any): boolean;
}
export declare class SurveyError {
        getText(): string;
}
export declare class SurveyElement extends Base implements ISurveyElement {
        name: string;
        errorsChangedCallback: () => void;
        static ScrollElementToTop(elementId: string): boolean;
        static GetFirstNonTextElement(elements: any): any;
        static FocusElement(elementId: string): boolean;
        static setVisibleIndex(questions: Array<IQuestion>, index: number, showIndex: boolean): number;
        constructor(name: string);
        setSurveyImpl(value: ISurveyImpl): void;
        protected readonly surveyImpl: ISurveyImpl;
        readonly data: ISurveyData;
        /**
            * Returns the survey object.
            */
        readonly survey: ISurvey;
        /**
            * Returns true if the question in design mode right now.
            */
        readonly isDesignMode: boolean;
        readonly isLoadingFromJson: boolean;
        /**
            * The list of errors. It is created by callig hasErrors functions
            * @see hasErrors
            */
        errors: Array<SurveyError>;
        getElementsInDesign(includeHidden?: boolean): Array<IElement>;
        selectedElementInDesign: SurveyElement;
        updateCustomWidgets(): void;
        onSurveyLoad(): void;
        endLoadingFromJson(): void;
        protected readonly textProcessor: ITextProcessor;
        protected getProcessedHtml(html: string): string;
        protected onSetData(): void;
        protected getPage(parent: IPanel): IPage;
        protected setPage(parent: IPanel, val: IPage): void;
        protected copyCssClasses(dest: any, source: any): void;
}
export declare class Event<T extends Function, Options> {
        readonly isEmpty: boolean;
        fire(sender: any, options: Options): void;
        add(func: T): void;
        remove(func: T): void;
}

export interface ILocalizableOwner {
    getLocale(): string;
    getMarkdownHtml(text: string): string;
}
/**
  * The class represents the string that supports multi-languages and markdown.
  * It uses in all objects where support for multi-languages and markdown is required.
  */
export declare class LocalizableString {
    owner: ILocalizableOwner;
    useMarkdown: boolean;
    static defaultLocale: string;
    onRenderedHtmlCallback: (html: string) => string;
    onGetTextCallback: (str: string) => string;
    constructor(owner: ILocalizableOwner, useMarkdown?: boolean);
    readonly locale: string;
    text: string;
    readonly pureText: any;
    readonly hasHtml: boolean;
    readonly html: string;
    readonly textOrHtml: string;
    readonly renderedHtml: string;
    getLocaleText(loc: string): string;
    setLocaleText(loc: string, value: string): void;
    getJson(): any;
    setJson(value: any): void;
    onChanged(): void;
    protected onCreating(): void;
}

/**
  * A definition for filling choices for checkbox, dropdown and radiogroup questions from resfull services.
  * The run method call a restfull service and results can be get on getREsultCallback.
  */
export declare class ChoicesRestfull extends Base {
    static onBeforeSendRequest: (sender: ChoicesRestfull, options: {
        request: XMLHttpRequest;
    }) => void;
    protected processedUrl: string;
    protected processedPath: string;
    getResultCallback: (items: Array<ItemValue>) => void;
    error: SurveyError;
    owner: Base;
    constructor();
    run(textProcessor?: ITextProcessor): void;
    protected useChangedItemsResults(): boolean;
    protected parseResponse(response: any): any;
    protected sendRequest(): void;
    getType(): string;
    readonly isEmpty: boolean;
    getCustomPropertiesNames(): Array<string>;
    setData(json: any): void;
    getData(): any;
    url: string;
    path: string;
    valueName: string;
    titleName: string;
    readonly itemValueType: string;
    clear(): void;
    protected onLoad(result: any): void;
}

export declare class FunctionFactory {
    static Instance: FunctionFactory;
    register(name: string, func: (params: any[]) => any): void;
    clear(): void;
    getAll(): Array<string>;
    run(name: string, params: any[]): any;
}

export declare class Operand {
    origionalValue: any;
    constructor(origionalValue: any);
    getValue(processValue: ProcessValue): any;
    readonly isBoolean: boolean;
    toString(): string;
}
export declare class FunctionOperand extends Operand {
    origionalValue: any;
    parameters: Array<Operand>;
    constructor(origionalValue: any);
    getValue(processValue: ProcessValue): any;
    toString(): string;
}
export declare class ExpressionOperand extends Operand {
    left: Operand;
    right: Operand;
    operator: string;
    constructor();
    getValue(processValue: ProcessValue): any;
    toString(): string;
}
export declare class ConditionOperand extends Operand {
    root: ConditionNode;
    constructor(root?: ConditionNode);
    getValue(processValue: ProcessValue): any;
    toString(): string;
}
export declare class Condition {
    static operatorsValue: HashTable<Function>;
    static readonly operators: HashTable<Function>;
    static getOperator(opName: string): any;
    static setOperator(opName: string, func: (left: any, right: any) => boolean): void;
    static isCorrectOperator(opName: string): boolean;
    static isNoRightOperation(op: string): boolean;
    left: Operand;
    right: Operand;
    operator: string;
    perform(left?: any, right?: any, processValue?: ProcessValue): boolean;
    performExplicit(left: any, right: any, processValue: ProcessValue): boolean;
    toString(): string;
}
export declare class ConditionNode {
    children: Array<any>;
    constructor();
    connective: string;
    readonly isEmpty: boolean;
    clear(): void;
    toString(): string;
}
export declare class ExpressionRunner {
    constructor(expression: string);
    expression: string;
    run(values: HashTable<any>): any;
}
export declare class ConditionRunner {
    constructor(expression: string);
    expression: string;
    run(values: HashTable<any>): boolean;
}

export declare class ConditionsParser {
    parse(text: string, root: ConditionNode): boolean;
    parseExpression(text: string): Operand;
    toString(root: ConditionNode): string;
}

export declare class ProcessValue {
    values: HashTable<any>;
    constructor();
    getFirstName(text: string): string;
    hasValue(text: string, values?: HashTable<any>): boolean;
    getValue(text: string, values?: HashTable<any>): any;
}

export declare class AnswerRequiredError extends SurveyError {
    customErrorText: string;
    constructor(customErrorText?: string);
    getText(): string;
}
export declare class OneAnswerRequiredError extends SurveyError {
    customErrorText: string;
    constructor(customErrorText?: string);
    getText(): string;
}
export declare class RequreNumericError extends SurveyError {
    constructor();
    getText(): string;
}
export declare class ExceedSizeError extends SurveyError {
    constructor(maxSize: number);
    getText(): string;
}
export declare class CustomError extends SurveyError {
    constructor(text: string);
    getText(): string;
}

export declare class JsonObjectProperty {
    name: string;
    isSerializable: boolean;
    className: string;
    alternativeName: string;
    classNamePart: string;
    baseClassName: string;
    defaultValue: any;
    serializationProperty: string;
    onGetValue: (obj: any) => any;
    onSetValue: (obj: any, value: any, jsonConv: JsonObject) => any;
    constructor(name: string, isRequired?: boolean);
    type: string;
    readonly isRequired: boolean;
    readonly hasToUseGetValue: string | ((obj: any) => any);
    isDefaultValue(value: any): boolean;
    getValue(obj: any): any;
    getPropertyValue(obj: any): any;
    readonly hasToUseSetValue: string | ((obj: any, value: any, jsonConv: JsonObject) => any);
    setValue(obj: any, value: any, jsonConv: JsonObject): void;
    getObjType(objType: string): string;
    getClassName(className: string): string;
    readonly choices: Array<any>;
    getChoices(obj: any): Array<any>;
    setChoices(value: Array<any>, valueFunc: () => Array<any>): void;
    readOnly: boolean;
    visible: boolean;
    isLocalizable: boolean;
    mergeWith(prop: JsonObjectProperty): void;
}
export declare class CustomPropertiesCollection {
    static addProperty(className: string, property: any): void;
    static removeProperty(className: string, propertyName: string): void;
    static addClass(className: string, parentClassName: string): void;
    static getProperties(className: string): Array<any>;
    static createProperties(obj: any): void;
}
export declare class JsonMetadataClass {
    name: string;
    creator: () => any;
    parentName: string;
    static requiredSymbol: string;
    static typeSymbol: string;
    properties: Array<JsonObjectProperty>;
    requiredProperties: Array<string>;
    constructor(name: string, properties: Array<any>, creator?: () => any, parentName?: string);
    find(name: string): JsonObjectProperty;
    createProperty(propInfo: any): JsonObjectProperty;
}
export declare class JsonMetadata {
    addClass(name: string, properties: Array<any>, creator?: () => any, parentName?: string): JsonMetadataClass;
    overrideClassCreatore(name: string, creator: () => any): void;
    getProperties(className: string): Array<JsonObjectProperty>;
    getPropertiesByObj(obj: any): Array<JsonObjectProperty>;
    findProperty(className: string, propertyName: string): JsonObjectProperty;
    findProperties(className: string, propertyNames: Array<string>): Array<JsonObjectProperty>;
    createClass(name: string): any;
    getChildrenClasses(name: string, canBeCreated?: boolean): Array<JsonMetadataClass>;
    getRequiredProperties(name: string): Array<string>;
    addProperties(className: string, propertiesInfos: Array<any>): void;
    addProperty(className: string, propertyInfo: any): void;
    removeProperty(className: string, propertyName: string): boolean;
    findClass(name: string): JsonMetadataClass;
}
export declare class JsonError {
    type: string;
    message: string;
    description: string;
    at: Number;
    constructor(type: string, message: string);
    getFullDescription(): string;
}
export declare class JsonUnknownPropertyError extends JsonError {
    propertyName: string;
    className: string;
    constructor(propertyName: string, className: string);
}
export declare class JsonMissingTypeErrorBase extends JsonError {
    baseClassName: string;
    type: string;
    message: string;
    constructor(baseClassName: string, type: string, message: string);
}
export declare class JsonMissingTypeError extends JsonMissingTypeErrorBase {
    propertyName: string;
    baseClassName: string;
    constructor(propertyName: string, baseClassName: string);
}
export declare class JsonIncorrectTypeError extends JsonMissingTypeErrorBase {
    propertyName: string;
    baseClassName: string;
    constructor(propertyName: string, baseClassName: string);
}
export declare class JsonRequiredPropertyError extends JsonError {
    propertyName: string;
    className: string;
    constructor(propertyName: string, className: string);
}
export declare class JsonObject {
    static readonly metaData: JsonMetadata;
    errors: JsonError[];
    toJsonObject(obj: any): any;
    toObject(jsonObj: any, obj: any): void;
    protected toJsonObjectCore(obj: any, property: JsonObjectProperty): any;
    protected valueToJson(obj: any, result: any, property: JsonObjectProperty): void;
    protected valueToObj(value: any, obj: any, key: any, property: JsonObjectProperty): void;
}

export interface IMatrixDropdownData {
        onRowChanged(row: MatrixDropdownRowModelBase, columnName: string, newRowValue: any): any;
        validateCell(row: MatrixDropdownRowModelBase, columnName: string, rowValue: any): SurveyError;
        columns: Array<MatrixDropdownColumn>;
        createQuestion(row: MatrixDropdownRowModelBase, column: MatrixDropdownColumn): Question;
        getLocale(): string;
        getMarkdownHtml(text: string): string;
        getSurvey(): ISurvey;
}
export interface IMatrixColumnOwner extends ILocalizableOwner {
        getRequiredText(): string;
        onColumnPropertiesChanged(column: MatrixDropdownColumn): any;
        getCellType(): string;
}
export declare var matrixDropdownColumnTypes: {
        dropdown: {
                properties: string[];
                onCellQuestionUpdate: (cellQuestion: any, column: any, question: any, data: any) => void;
        };
        checkbox: {
                properties: string[];
                onCellQuestionUpdate: (cellQuestion: any, column: any, question: any, data: any) => void;
        };
        radiogroup: {
                properties: string[];
                onCellQuestionUpdate: (cellQuestion: any, column: any, question: any, data: any) => void;
        };
        text: {
                properties: string[];
                onCellQuestionUpdate: (cellQuestion: any, column: any, question: any, data: any) => void;
        };
        comment: {
                properties: string[];
                onCellQuestionUpdate: (cellQuestion: any, column: any, question: any, data: any) => void;
        };
        boolean: {
                properties: string[];
                onCellQuestionUpdate: (cellQuestion: any, column: any, question: any, data: any) => void;
        };
        expression: {
                properties: string[];
                onCellQuestionUpdate: (cellQuestion: any, column: any, question: any, data: any) => void;
        };
};
export declare class MatrixDropdownColumn extends Base implements ILocalizableOwner {
        static getColumnTypes(): Array<string>;
        constructor(name: string, title?: string);
        getDynamicPropertyName(): string;
        getDynamicType(): string;
        getDynamicProperties(): Array<string>;
        colOwner: IMatrixColumnOwner;
        getType(): string;
        cellType: string;
        readonly templateQuestion: Question;
        name: string;
        title: string;
        readonly locTitle: LocalizableString;
        readonly fullTitle: string;
        getFullTitle(str: string): string;
        isRequired: boolean;
        readOnly: boolean;
        hasOther: boolean;
        visibleIf: string;
        enableIf: string;
        validators: Array<SurveyValidator>;
        minWidth: string;
        colCount: number;
        getLocale(): string;
        getMarkdownHtml(text: string): string;
        createCellQuestion(data: any): Question;
        updateCellQuestion(cellQuestion: Question, data: any): void;
        defaultCellTypeChanged(): void;
        protected calcCellQuestionType(): string;
        protected updateTemplateQuestion(): void;
        protected createNewQuestion(cellType: string): Question;
        protected setQuestionProperties(question: Question): void;
        protected propertyValueChanged(name: string, oldValue: any, newValue: any): void;
}
export declare class MatrixDropdownCell {
        column: MatrixDropdownColumn;
        row: MatrixDropdownRowModelBase;
        data: IMatrixDropdownData;
        constructor(column: MatrixDropdownColumn, row: MatrixDropdownRowModelBase, data: IMatrixDropdownData);
        readonly question: Question;
        value: any;
        runCondition(values: HashTable<any>): void;
}
export declare class MatrixDropdownRowModelBase implements ISurveyData, ISurveyImpl, ILocalizableOwner, ITextProcessor {
        protected data: IMatrixDropdownData;
        cells: Array<MatrixDropdownCell>;
        constructor(data: IMatrixDropdownData, value: any);
        readonly id: string;
        readonly rowName: any;
        value: any;
        getAllValues(): any;
        onAnyValueChanged(name: string): void;
        getValue(name: string): any;
        setValue(name: string, newValue: any): void;
        getComment(name: string): string;
        setComment(name: string, newValue: string): void;
        readonly isEmpty: boolean;
        getQuestionByColumn(column: MatrixDropdownColumn): Question;
        getLocale(): string;
        getMarkdownHtml(text: string): string;
        onLocaleChanged(): void;
        runCondition(values: HashTable<any>): void;
        protected buildCells(): void;
        protected createCell(column: MatrixDropdownColumn): MatrixDropdownCell;
        geSurveyData(): ISurveyData;
        getSurvey(): ISurvey;
        getTextProcessor(): ITextProcessor;
        processText(text: string, returnDisplayValue: boolean): string;
        processTextEx(text: string): any;
}
/**
    * A base class for matrix dropdown and matrix dynamic questions.
    */
export declare class QuestionMatrixDropdownModelBase extends Question implements IMatrixDropdownData {
        name: string;
        static addDefaultColumns(matrix: QuestionMatrixDropdownModelBase): void;
        protected generatedVisibleRows: Array<MatrixDropdownRowModelBase>;
        columnsChangedCallback: () => void;
        updateCellsCallback: () => void;
        constructor(name: string);
        getType(): string;
        readonly isAllowTitleLeft: boolean;
        /**
            * The list of matrix columns.
            */
        columns: Array<MatrixDropdownColumn>;
        protected onMatrixRowCreated(row: MatrixDropdownRowModelBase): void;
        /**
            * Use this property to change the default cell type.
            */
        cellType: string;
        /**
            * The default column count for radiogroup and checkbox  cell types.
            */
        columnColCount: number;
        /**
            * Use this property to set the mimimum column width.
            */
        columnMinWidth: string;
        /**
            * Set this property to true to show the horizontal scroll.
            */
        horizontalScroll: boolean;
        getRequiredText(): string;
        onColumnPropertiesChanged(column: MatrixDropdownColumn): void;
        getCellType(): string;
        runCondition(values: HashTable<any>): void;
        protected runCellsCondition(values: HashTable<any>): void;
        onLocaleChanged(): void;
        /**
            * Returns the column by it's name. Retuns null if a column with this name doesn't exist.
            * @param column
            */
        getColumnName(columnName: string): MatrixDropdownColumn;
        /**
            * Returns the column width.
            * @param column
            */
        getColumnWidth(column: MatrixDropdownColumn): string;
        /**
            * The default choices for dropdown, checkbox and radiogroup cell types.
            */
        choices: Array<any>;
        /**
            * The default options caption for dropdown cell type.
            */
        optionsCaption: string;
        readonly locOptionsCaption: LocalizableString;
        addColumn(name: string, title?: string): MatrixDropdownColumn;
        /**
            * Returns the rows model objects that used during rendering.
            */
        readonly visibleRows: Array<MatrixDropdownRowModelBase>;
        onSurveyLoad(): void;
        /**
            * Returns the row value. If the row value is empty, the object is empty: {}.
            * @param rowIndex row index from 0 to visible row count - 1.
            */
        getRowValue(rowIndex: number): any;
        /**
            * Set the row value.
            * @param rowIndex row index from 0 to visible row count - 1.
            * @param rowValue an object {"column name": columnValue,... }
            */
        setRowValue(rowIndex: number, rowValue: any): any;
        protected generateRows(): Array<MatrixDropdownRowModelBase>;
        protected createNewValue(curValue: any): any;
        protected getRowValueCore(row: MatrixDropdownRowModelBase, questionValue: any, create?: boolean): any;
        protected getRowDisplayValue(row: MatrixDropdownRowModelBase, rowValue: any): any;
        protected onBeforeValueChanged(val: any): void;
        protected onValueChanged(): void;
        supportGoNextPageAutomatic(): boolean;
        hasErrors(fireCallback?: boolean): boolean;
        getAllErrors(): Array<SurveyError>;
        protected getFirstInputElementId(): string;
        protected getFirstErrorInputElementId(): string;
        protected getFirstCellQuestion(onError: boolean): Question;
        createQuestion(row: MatrixDropdownRowModelBase, column: MatrixDropdownColumn): Question;
        protected createQuestionCore(row: MatrixDropdownRowModelBase, column: MatrixDropdownColumn): Question;
        protected deleteRowValue(newValue: any, row: MatrixDropdownRowModelBase): any;
        onAnyValueChanged(name: string): void;
        protected onCellValueChanged(row: MatrixDropdownRowModelBase, columnName: string, rowValue: any): void;
        validateCell(row: MatrixDropdownRowModelBase, columnName: string, rowValue: any): SurveyError;
        onRowChanged(row: MatrixDropdownRowModelBase, columnName: string, newRowValue: any): void;
        getSurvey(): ISurvey;
}

export declare class MatrixDropdownRowModel extends MatrixDropdownRowModelBase {
        name: string;
        constructor(name: string, item: ItemValue, data: IMatrixDropdownData, value: any);
        readonly rowName: string;
        readonly text: string;
        readonly locText: LocalizableString;
}
/**
    * A Model for a matrix dropdown question. You may use a dropdown, checkbox, radiogroup, text and comment questions as a cell editors.
    */
export declare class QuestionMatrixDropdownModel extends QuestionMatrixDropdownModelBase implements IMatrixDropdownData {
        name: string;
        constructor(name: string);
        getType(): string;
        readonly displayValue: any;
        /**
            * The list of rows. A row has a value and an optional text
            */
        rows: Array<any>;
        protected generateRows(): Array<MatrixDropdownRowModel>;
        protected createMatrixRow(item: ItemValue, value: any): MatrixDropdownRowModel;
}

export declare class MatrixDynamicRowModel extends MatrixDropdownRowModelBase {
        index: number;
        constructor(index: number, data: IMatrixDropdownData, value: any);
        readonly rowName: string;
}
/**
    * A Model for a matrix dymanic question. You may use a dropdown, checkbox, radiogroup, text and comment questions as a cell editors.
    * An end-user may dynamically add/remove rows, unlike in matrix dropdown question.
    */
export declare class QuestionMatrixDynamicModel extends QuestionMatrixDropdownModelBase implements IMatrixDropdownData {
        name: string;
        static MaxRowCount: number;
        rowCountChangedCallback: () => void;
        constructor(name: string);
        getType(): string;
        /**
            * Set it to true, to show a confirmation dialog on removing a row
            * @see ConfirmDeleteText
            */
        confirmDelete: boolean;
        /**
            * Set it to a column name and the library shows duplication error, if there are same values in different rows in the column.
            * @see keyDuplicationError
            */
        keyName: string;
        /**
            * The number of rows in the matrix.
            * @see minRowCount
            * @see maxRowCount
            */
        rowCount: number;
        /**
            * The minimum row count. A user could not delete a row if the rowCount equals to minRowCount
            * @see rowCount
            * @see maxRowCount
            */
        minRowCount: number;
        /**
            * The maximum row count. A user could not add a row if the rowCount equals to maxRowCount
            * @see rowCount
            * @see minRowCount
            */
        maxRowCount: number;
        /**
            * Returns true, if a new row can be added.
            * @see maxRowCount
            * @see canRemoveRow
            * @see rowCount
            */
        readonly canAddRow: boolean;
        /**
            * Returns true, if a row can be removed.
            * @see minRowCount
            * @see canAddRow
            * @see rowCount
            */
        readonly canRemoveRow: boolean;
        /**
            * Creates and add a new row.
            */
        addRow(): void;
        /**
            * Removes a row by it's index. If confirmDelete is true, show a confirmation dialog
            * @param index a row index, from 0 to rowCount - 1
            * @see removeRow
            * @see confirmDelete
            */
        removeRowUI(value: any): void;
        /**
            * Removes a row by it's index.
            * @param index a row index, from 0 to rowCount - 1
            */
        removeRow(index: number): void;
        /**
            * Use this property to change the default text showing in the confirmation delete dialog on removing a row.
            */
        confirmDeleteText: string;
        readonly locConfirmDeleteText: LocalizableString;
        /**
            * The duplication value error text. Set it to show the text different from the default.
            * @see keyName
            */
        keyDuplicationError: string;
        readonly locKeyDuplicationError: LocalizableString;
        /**
            * Use this property to change the default value of add row button text.
            */
        addRowText: string;
        readonly locAddRowText: LocalizableString;
        /**
            * Use this property to change the default value of remove row button text.
            */
        removeRowText: string;
        readonly locRemoveRowText: LocalizableString;
        readonly displayValue: any;
        supportGoNextPageAutomatic(): boolean;
        protected onCheckForErrors(errors: Array<SurveyError>): void;
        hasErrors(fireCallback?: boolean): boolean;
        protected generateRows(): Array<MatrixDynamicRowModel>;
        protected createMatrixRow(value: any): MatrixDynamicRowModel;
        protected onBeforeValueChanged(val: any): void;
        protected createNewValue(curValue: any): any;
        protected deleteRowValue(newValue: any, row: MatrixDropdownRowModelBase): any;
        protected getRowValueCore(row: MatrixDropdownRowModelBase, questionValue: any, create?: boolean): any;
}

export interface IMatrixData {
        onMatrixRowChanged(row: MatrixRowModel): any;
}
export declare class MatrixRowModel {
        fullName: string;
        protected rowValue: any;
        constructor(item: ItemValue, fullName: string, data: IMatrixData, value: any);
        readonly name: string;
        readonly text: string;
        readonly locText: LocalizableString;
        value: any;
        protected onValueChanged(): void;
}
/**
    * A Model for a simple matrix question.
    */
export declare class QuestionMatrixModel extends Question implements IMatrixData {
        name: string;
        constructor(name: string);
        getType(): string;
        readonly isAllowTitleLeft: boolean;
        /**
            * Set this property to true, if you want a user to answer all rows.
            */
        isAllRowRequired: boolean;
        /**
            * Returns true, if there is at least one row.
            */
        readonly hasRows: boolean;
        /**
            * The list of columns. A column has a value and an optional text
            */
        columns: Array<any>;
        /**
            * The list of rows. A row has a value and an optional text
            */
        rows: Array<any>;
        /**
            * Returns the list of rows as model objects.
            */
        readonly visibleRows: Array<MatrixRowModel>;
        supportGoNextPageAutomatic(): boolean;
        protected onCheckForErrors(errors: Array<SurveyError>): void;
        protected createMatrixRow(item: ItemValue, fullName: string, value: any): MatrixRowModel;
        protected onValueChanged(): void;
        readonly displayValue: any;
        onMatrixRowChanged(row: MatrixRowModel): void;
}

export interface IMultipleTextData {
        getSurvey(): ISurvey;
        getTextProcessor(): ITextProcessor;
        getAllValues(): any;
        getMultipleTextValue(name: string): any;
        setMultipleTextValue(name: string, value: any): any;
        getIsRequiredText(): string;
        getLocale(): string;
        getMarkdownHtml(text: string): string;
}
export declare class MultipleTextItemModel extends Base implements IValidatorOwner, ISurveyData, ISurveyImpl {
        valueChangedCallback: (newValue: any) => void;
        validators: Array<SurveyValidator>;
        constructor(name?: any, title?: string);
        getType(): string;
        readonly id: string;
        /**
            * The item name.
            */
        name: string;
        readonly editor: QuestionTextModel;
        protected createEditor(name: string): QuestionTextModel;
        setData(data: IMultipleTextData): void;
        /**
            * Set this property to true, to make the item a required. If a user doesn't fill the item then a validation error will be generated.
            */
        isRequired: boolean;
        /**
            * Use this property to change the default input type.
            */
        inputType: string;
        /**
            * Item title. If it is empty, the item name is rendered as title. This property supports markdown.
            * @see name
            */
        title: string;
        readonly locTitle: LocalizableString;
        /**
            * Returns the text or html for rendering the title.
            */
        readonly fullTitle: string;
        /**
            * The input place holder.
            */
        placeHolder: string;
        readonly locPlaceHolder: LocalizableString;
        /**
            * The item value.
            */
        value: any;
        onValueChanged(newValue: any): void;
        geSurveyData(): ISurveyData;
        getSurvey(): ISurvey;
        getTextProcessor(): ITextProcessor;
        getValue(name: string): any;
        setValue(name: string, value: any): void;
        getComment(name: string): string;
        setComment(name: string, newValue: string): void;
        getAllValues(): any;
        getValidatorTitle(): string;
        validatedValue: any;
}
/**
    * A Model for a multiple text question.
    */
export declare class QuestionMultipleTextModel extends Question implements IMultipleTextData {
        name: string;
        colCountChangedCallback: () => void;
        constructor(name: string);
        getType(): string;
        setSurveyImpl(value: ISurveyImpl): void;
        readonly isAllowTitleLeft: boolean;
        endLoadingFromJson(): void;
        /**
            * The list of input items.
            */
        items: Array<MultipleTextItemModel>;
        /**
            * Add a new text item.
            * @param name a item name
            * @param title a item title (optional)
            */
        addItem(name: string, title?: string): MultipleTextItemModel;
        onLocaleChanged(): void;
        supportGoNextPageAutomatic(): boolean;
        /**
            * The number of columns. Items are rendred in one line if the value is 0.
            */
        colCount: number;
        /**
            * The default text input size.
            */
        itemSize: number;
        /**
            * Returns the list of rendered rows.
            */
        getRows(): Array<any>;
        protected onValueChanged(): void;
        protected createTextItem(name: string, title: string): MultipleTextItemModel;
        protected onItemValueChanged(): void;
        protected runValidators(): SurveyError;
        protected onCheckForErrors(errors: Array<SurveyError>): void;
        getMultipleTextValue(name: string): any;
        setMultipleTextValue(name: string, value: any): void;
        getSurvey(): ISurvey;
        getTextProcessor(): ITextProcessor;
        getAllValues(): any;
        getIsRequiredText(): string;
}

export declare class QuestionRowModel {
        panel: PanelModelBase;
        visibilityChangedCallback: () => void;
        constructor(panel: PanelModelBase);
        elements: Array<IElement>;
        readonly questions: Array<IElement>;
        visible: boolean;
        updateVisible(): void;
        addElement(q: IElement): void;
        protected onVisibleChanged(): void;
}
/**
    * A base class for a Panel and Page objects.
    */
export declare class PanelModelBase extends SurveyElement implements IPanel, IConditionRunner, ILocalizableOwner {
        name: string;
        rowsChangedCallback: () => void;
        onGetQuestionTitleLocation: () => string;
        constructor(name?: string);
        setSurveyImpl(value: ISurveyImpl): void;
        endLoadingFromJson(): void;
        /**
            * PanelModel or PageModel title property.
            * @description
            */
        title: string;
        readonly locTitle: LocalizableString;
        /**
            * PanelModel or PageModel description property. It renders under title by using smaller font. Unlike the title, description can be empty.
            * @see title
            */
        description: string;
        readonly locDescription: LocalizableString;
        readonly hasDescription: boolean;
        /**
            * The custom text that will be shown on required error. Use this property, if you do not want to show the default text.
            */
        requiredErrorText: string;
        readonly locRequiredErrorText: LocalizableString;
        getLocale(): string;
        getMarkdownHtml(text: string): string;
        /**
            * A parent element. It is always null for the Page object and always not null for the Panel object. Panel object may contain Questions and other Panels.
            */
        parent: PanelModelBase;
        /**
            * An expression that returns true or false. If it returns true the Panel becomes visible and if it returns false the Panel becomes invisible. The library runs the expression on survey start and on changing a question value. If the property is empty then visible property is used.
            * @see visible
            */
        visibleIf: string;
        readonly cssClasses: any;
        /**
            * A unique element identificator. It is generated automatically.
            */
        id: string;
        /**
            * Returns true if the current object is Panel. Returns false if the current object is Page (a root Panel).
            */
        readonly isPanel: boolean;
        /**
            * Returns the list of all questions located in the Panel/Page, including in the nested Panels.
            * @see QuestionBase
            * @see elements
            */
        readonly questions: Array<QuestionBase>;
        getQuestionByName(name: string): QuestionBase;
        getQuestionByValueName(valueName: string): QuestionBase;
        /**
            * Returns the list of the elements in the object, Panel/Page. Elements can be questions or panels. The function doesn't return elements in the nested Panels.
            */
        readonly elements: Array<IElement>;
        getElementsInDesign(includeHidden?: boolean): Array<IElement>;
        /**
            * Returns true if the current element belongs to the Panel/Page. It looks in nested Panels as well.
            * @param element
            * @see PanelModel
            */
        containsElement(element: IElement): boolean;
        /**
            * Set this property to true, to require the answer at least in one question in the panel.
            */
        isRequired: boolean;
        /**
            * Returns true, if there is an error on this Page or inside the current Panel
            * @param fireCallback set it to true, to show errors in UI
            * @param focuseOnFirstError set it to true to focuse on the first question that doesn't pass the validation
            */
        hasErrors(fireCallback?: boolean, focuseOnFirstError?: boolean): boolean;
        protected hasErrorsCore(rec: any): void;
        /**
            * Fill list array with the questions.
            * @param list
            * @param visibleOnly set it to true to get visible questions only
            */
        addQuestionsToList(list: Array<IQuestion>, visibleOnly?: boolean, includingDesignTime?: boolean): void;
        /**
            * Fill list array with the panels.
            * @param list
            */
        addPanelsIntoList(list: Array<IPanel>, visibleOnly?: boolean, includingDesignTime?: boolean): void;
        readonly rows: Array<QuestionRowModel>;
        /**
            * Returns true if the current object is Page and it is the current page.
            */
        readonly isActive: boolean;
        updateCustomWidgets(): void;
        /**
            * Set this property different from "default" to set the specific question title location for this panel/page.
            * @see SurveyModel.questionTitleLocation
            */
        questionTitleLocation: string;
        getQuestionTitleLocation(): string;
        protected readonly root: PanelModelBase;
        protected createRow(): QuestionRowModel;
        onSurveyLoad(): void;
        protected onRowsChanged(): void;
        /**
            * Returns rendered title text or html.
            */
        readonly processedTitle: string;
        protected getRenderedTitle(str: string): string;
        /**
            * Use it to get/set the object visibility.
            * @see visibleIf
            */
        visible: boolean;
        protected onVisibleChanged(): void;
        /**
            * Returns true if object is visible or survey is in design mode right now.
            */
        readonly isVisible: boolean;
        getIsPageVisible(exceptionQuestion: IQuestion): boolean;
        /**
            * Add an elememnt into Panel or Page.
            * @param element
            * @param index element index in the elements array
            */
        addElement(element: IElement, index?: number): void;
        /**
            * Add a question into Panel or Page.
            * @param question
            * @param index element index in the elements array
            */
        addQuestion(question: QuestionBase, index?: number): void;
        /**
            * Add a panel into Panel or Page.
            * @param panel
            * @param index element index in the elements array
            */
        addPanel(panel: PanelModel, index?: number): void;
        /**
            * Creates a new question and adds it into the end of the elements list.
            * @param questionType the possible values are: "text", "checkbox", "dropdown", "matrix", "html", "matrixdynamic", "matrixdropdown" and so on.
            * @param name a question name
            */
        addNewQuestion(questionType: string, name?: string): QuestionBase;
        /**
            * Creates a new panel and adds it into the end of the elements list.
            * @param name a panel name
            */
        addNewPanel(name?: string): PanelModel;
        protected createNewPanel(name: string): PanelModel;
        /**
            * Remove an element (Panel or Question) from the elements list.
            * @param element
            * @see elements
            */
        removeElement(element: IElement): boolean;
        /**
            * Remove question  from the elements list.
            * @param question
            * @see elements
            * @see removeElement
            */
        removeQuestion(question: QuestionBase): void;
        runCondition(values: HashTable<any>): void;
        onLocaleChanged(): void;
        onAnyValueChanged(name: string): void;
}
/**
    * A container element, similar to the Page objects. However, unlike the Page, Panel can't be a root.
    * It may contain questions and other panels.
    */
export declare class PanelModel extends PanelModelBase implements IElement {
        name: string;
        stateChangedCallback: () => void;
        constructor(name?: string);
        getType(): string;
        readonly isPanel: boolean;
        /**
            * Get/set the page where the panel is located.
            */
        page: IPage;
        /**
            * Set this property to "collapsed" to render only Panel title and expanded button and to "expanded" to render the collapsed button in the Panel caption
            */
        state: string;
        /**
            * Returns true if the Panel is in the collapsed state
            * @see state
            * @see collapse
            * @see isExpanded
            */
        readonly isCollapsed: boolean;
        /**
            * Returns true if the Panel is in the expanded state
            * @see state
            * @see expand
            * @see isCollapsed
            */
        readonly isExpanded: boolean;
        /**
            * Collapse the Panel
            * @see state
            */
        collapse(): void;
        /**
            * Expand the Panel
            * @see state
            */
        expand(): void;
        protected hasErrorsCore(rec: any): void;
        protected getRenderedTitle(str: string): string;
        /**
            * The Panel width.
            */
        width: string;
        /**
            * The inner indent. Set this property to increase the panel content margin.
            */
        innerIndent: number;
        renderWidth: string;
        /**
            * The Panel renders on the new line if the property is true. If the property is false, the panel tries to render on the same line/row with a previous question/panel.
            */
        startWithNewLine: boolean;
        /**
            * The right indent of the Panel.
            */
        rightIndent: number;
        protected onVisibleChanged(): void;
}

/**
    * The page object. It has elements collection, that contains questions and panels.
    */
export declare class PageModel extends PanelModelBase implements IPage {
        name: string;
        constructor(name?: string);
        getType(): string;
        toString(): string;
        /**
            * The visible index of the page. It has values from 0 to visible page count - 1.
            * @see SurveyModel.visiblePages
            * @see SurveyModel.pages
            */
        visibleIndex: number;
        /**
            * Returns true, if the page is started page in the survey. It can be shown on the start only and the end-user could not comeback to it after it passed it.
            */
        readonly isStarted: boolean;
        getIsPageVisible(exceptionQuestion: IQuestion): boolean;
        num: number;
        /**
            * Set this property to "hide" to make "Prev", "Next" and "Complete" buttons are invisible for this page. Set this property to "show" to make these buttons visible, even if survey showNavigationButtons property is false.
            * @see SurveyMode.showNavigationButtons
            */
        navigationButtonsVisibility: string;
        protected getRenderedTitle(str: string): string;
        /**
            * The property returns true, if the page has been shown to the end-user.
            */
        readonly wasShown: boolean;
        readonly hasShown: boolean;
        setWasShown(val: boolean): void;
        /**
            * The property returns true, if the elements are randomized on the page
            * @see hasShown
            * @see questionsOrder
            * @see SurveyModel.questionsOrder
            */
        readonly areQuestionsRandomized: boolean;
        /**
            * Use this property to randomize questions. Set it to 'random' to randomize questions, 'initial' to keep them in the same order or 'default' to use the Survey questionsOrder property
            * @see SurveyModel.questionsOrder
            * @see areQuestionsRandomized
            */
        questionsOrder: string;
        /**
            * Call it to focus the input on the first question
            */
        focusFirstQuestion(): void;
        /**
            * Call it to focus the input of the first question that has an error.
            */
        focusFirstErrorQuestion(): void;
        /**
            * Call it to scroll to the page top.
            */
        scrollToTop(): void;
        /**
            * Time in seconds end-user spent on this page
            */
        timeSpent: number;
        /**
            * The maximum time in seconds that end-user has to complete the page. If the value is 0 or less, the end-user has unlimited number of time to finish the page.
            * @see startTimer
            * @see SurveyModel.maxTimeToFinishPage
            */
        maxTimeToFinish: number;
        protected onNumChanged(value: number): void;
        protected onVisibleChanged(): void;
}

/**
    * Extends question base class with title, value, errors and other functionality
    */
export declare class Question extends QuestionBase implements IValidatorOwner {
        name: string;
        valueChangedCallback: () => void;
        commentChangedCallback: () => void;
        titleChangedCallback: () => void;
        validateValueCallback: () => SurveyError;
        constructor(name: string);
        getValueName(): string;
        valueName: string;
        /**
            * Returns true if the question may have a title located on the left
            */
        readonly isAllowTitleLeft: boolean;
        getType(): string;
        readonly hasTitle: boolean;
        readonly hasDescription: boolean;
        /**
            * Set this property different from "default" to set the specific question title location for this panel/page.
            * @see SurveyModel.questionTitleLocation
            */
        titleLocation: string;
        /**
            * Return the title location based on question titleLocation property and QuestionTitleLocation of it's parents
            * @see titleLocation
            * @see PanelModelBase.QuestionTitleLocation
            * @see SurveyModel.QuestionTitleLocation
            */
        getTitleLocation(): string;
        protected getTitleLocationCore(): string;
        readonly hasTitleOnLeft: boolean;
        readonly hasTitleOnTop: boolean;
        readonly hasTitleOnBottom: boolean;
        readonly hasTitleOnLeftTop: boolean;
        readonly errorLocation: string;
        readonly hasInput: boolean;
        readonly inputId: string;
        /**
            * Question title. Use survey questionTitleTemplate property to change the title question is rendered. If it is empty, then question name property is used.
            * @see SurveyModel.questionTitleTemplate
            */
        title: string;
        readonly locTitle: LocalizableString;
        /**
            * Question description. It renders under question title by using smaller font. Unlike the title, description can be empty.
            * @see title
            */
        description: string;
        readonly locDescription: LocalizableString;
        /**
            * The custom text that will be shown on required error. Use this property, if you do not want to show the default text.
            */
        requiredErrorText: string;
        readonly locRequiredErrorText: LocalizableString;
        /**
            * Use it to get or set the comment value.
            */
        commentText: string;
        readonly locCommentText: LocalizableString;
        /**
            * Returns a copy of question errors survey. For some questions like matrix and panel dynamic it includes the errors of nested questions.
            */
        getAllErrors(): Array<SurveyError>;
        /**
            * Returns the rendred question title.
            */
        readonly processedTitle: string;
        /**
            * Returns the title after processing the question template.
            * @see SurveyModel.questionTitleTemplate
            */
        readonly fullTitle: string;
        focus(onError?: boolean): void;
        protected updateCssClasses(res: any, surveyCss: any): void;
        protected getFirstInputElementId(): string;
        protected getFirstErrorInputElementId(): string;
        protected getProcessedTextValue(textValue: TextPreProcessorValue): void;
        supportComment(): boolean;
        supportOther(): boolean;
        /**
            * Set this property to true, to make the question a required. If a user doesn't answer the question then a validation error will be generated.
            */
        isRequired: boolean;
        hasComment: boolean;
        hasOther: boolean;
        protected hasOtherChanged(): void;
        /**
            * Retuns true if readOnly property is true or survey is in display mode.
            * @see SurveyModel.model
            * @see readOnly
            */
        readonly isReadOnly: boolean;
        /**
            * Set it to true to make the question readonly.
            */
        readOnly: boolean;
        /**
            * An expression that returns true or false. If it returns false the Question becomes read only and an end-user will not able to answer on the qustion. The library runs the expression on survey start and on changing a question value. If the property is empty then readOnly property is used.
            * @see readOnly
            * @see isReadOnly
            */
        enableIf: string;
        runCondition(values: HashTable<any>): void;
        onReadOnlyChanged(): void;
        onAnyValueChanged(name: string): void;
        protected readonly no: string;
        onSurveyLoad(): void;
        protected onSetData(): void;
        /**
            * Get/Set the question value.
            * @see SurveyMode.setValue
            * @see SurveyMode.getValue
            */
        value: any;
        clearValue(): void;
        clearValueIfInvisible(): void;
        readonly displayValue: any;
        /**
            * Set the default value to the question. It will be assign to the question on loading the survey from JSON or adding a question to the survey or on setting this property of the value is empty.
            */
        defaultValue: any;
        /**
            * The correct answer on the question. Set this value if you are doing a quiz.
            * @see SurveyModel.correctAnswers
            * @see SurveyModel.inCorrectAnswers
            */
        correctAnswer: any;
        isAnswerCorrect(): boolean;
        protected updateValueWithDefaults(): void;
        protected setDefaultValue(): void;
        /**
            * The question comment value.
            */
        comment: string;
        protected getComment(): string;
        protected setComment(newValue: string): void;
        /**
            * Returns true if the question value is empty
            */
        isEmpty(): boolean;
        /**
            * The list of question validators.
            */
        validators: Array<SurveyValidator>;
        /**
            * Returns true if threre is a validation error(s) in the question.
            * @param fireCallback set it to true to show an error in UI.
            */
        hasErrors(fireCallback?: boolean): boolean;
        /**
            * Returns the validation errors count.
            */
        readonly currentErrorCount: number;
        /**
            * Returns the char/string for a required question.
            * @see SurveyModel.requiredText
            */
        readonly requiredText: string;
        /**
            * Add error into the question error list.
            * @param error
            */
        addError(error: SurveyError): void;
        protected onCheckForErrors(errors: Array<SurveyError>): void;
        protected hasRequiredError(): boolean;
        protected runValidators(): SurveyError;
        protected setNewValue(newValue: any): void;
        protected setNewValueInData(newValue: any): void;
        protected valueFromData(val: any): any;
        protected valueToData(val: any): any;
        protected onValueChanged(): void;
        protected setNewComment(newValue: string): void;
        onSurveyValueChanged(newValue: any): void;
        getValidatorTitle(): string;
        validatedValue: any;
}

/**
    * A base class for all questions. QuestionBase doesn't have information about title, values, errors and so on.
    * Those properties are defined in the Question class.
    */
export declare class QuestionBase extends SurveyElement implements IQuestion, IConditionRunner, ILocalizableOwner {
        name: string;
        customWidgetData: {
                isNeedRender: boolean;
        };
        /**
            * The event is fired when the survey change it's locale
            * @see SurveyModel.locale
            */
        localeChanged: Event<(sender: QuestionBase) => any, any>;
        focusCallback: () => void;
        surveyLoadCallback: () => void;
        constructor(name: string);
        getType(): string;
        setSurveyImpl(value: ISurveyImpl): void;
        /**
            * A parent element. It can be panel or page.
            */
        parent: IPanel;
        isAnswerCorrect(): boolean;
        getValueName(): string;
        /**
            * Get/set the page where the question is located.
            */
        page: IPage;
        /**
            * Always returns false.
            */
        readonly isPanel: boolean;
        /**
            * Use it to get/set the question visibility.
            * @see visibleIf
            */
        visible: boolean;
        protected onVisibleChanged(): void;
        /**
            * An expression that returns true or false. If it returns true the Question becomes visible and if it returns false the Question becomes invisible. The library runs the expression on survey start and on changing a question value. If the property is empty then visible property is used.
            * @see visible
            */
        visibleIf: string;
        /**
            * Returns true if the question is visible or survey is in design mode right now.
            */
        readonly isVisible: boolean;
        /**
            * Returns true if there is no input in the question. It always returns true for html question or survey is in 'display' mode.
            * @see QuestionHtmlModel
            * @see SurveyModel.mode
            * @see Question.readOnly
            */
        readonly isReadOnly: boolean;
        /**
            * Returns the visible index of the question in the survey. It can be from 0 to all visible questions count - 1
            */
        readonly visibleIndex: number;
        /**
            * Returns true if there is at least one error on question validation.
            * @param fireCallback set it to true to show error in UI
            */
        hasErrors(fireCallback?: boolean): boolean;
        /**
            * Returns true if the question value is empty
            */
        isEmpty(): boolean;
        /**
            * Returns the number of erros on validation.
            */
        readonly currentErrorCount: number;
        /**
            * Returns false if the question doesn't have a title property, for example: QuestionHtmlModel
            */
        readonly hasTitle: boolean;
        /**
            * Returns false if the question doesn't have a description property, for example: QuestionHtmlModel, or description property is empty.
            */
        readonly hasDescription: boolean;
        /**
            * Returns false if the question doesn't have an input element, for example: QuestionHtmlModel
            */
        readonly hasInput: boolean;
        /**
            * Returns true, if you can have a comment for the question.
            */
        readonly hasComment: boolean;
        /**
            * The unique identificator. It is generated automatically.
            */
        id: string;
        /**
            * Returns the list of errors that has in the question. For example, isRequired error.
            */
        getAllErrors(): Array<SurveyError>;
        /**
            * The link to the custom widget.
            */
        readonly customWidget: QuestionCustomWidget;
        updateCustomWidget(): void;
        /**
            * The Question renders on the new line if the property is true. If the property is false, the question tries to render on the same line/row with a previous question/panel.
            */
        startWithNewLine: boolean;
        /**
            * Returns all css classes that used for rendering the question. You may use survey.updateQuestionCssClasses event to override css classes for a question.
            * @see SurveyModel.updateQuestionCssClasses
            */
        readonly cssClasses: any;
        protected getRootCss(classes: any): any;
        protected updateCssClasses(res: any, surveyCss: any): void;
        /**
            * Use it to set the specific width to the question.
            */
        width: string;
        /**
            * The rendered width of the question.
            */
        renderWidth: string;
        /**
            * Set it different from 0 to increase the left padding.
            */
        indent: number;
        /**
            * Set it different from 0 to increase the right padding.
            */
        rightIndent: number;
        /**
            * Focus the question input.
            * @param onError Focus if there is an error.
            */
        focus(onError?: boolean): void;
        protected fireCallback(callback: () => void): void;
        protected onCreating(): void;
        /**
            * Run visibleIf and enableIf expressions. If visibleIf or/and enabledIf are not empty, then the results of performing the expression (true or false) set to the visible/readOnly properties.
            * @param values Typically survey results
            * @see visible
            * @see visibleIf
            * @see readOnly
            * @see enableIf
            */
        runCondition(values: HashTable<any>): void;
        onSurveyValueChanged(newValue: any): void;
        onSurveyLoad(): void;
        setVisibleIndex(val: number): number;
        removeElement(element: IElement): boolean;
        supportGoNextPageAutomatic(): boolean;
        clearUnusedValues(): void;
        readonly displayValue: any;
        clearValue(): void;
        clearValueIfInvisible(): void;
        onLocaleChanged(): void;
        onReadOnlyChanged(): void;
        onAnyValueChanged(name: string): void;
        locOwner: ILocalizableOwner;
        /**
            * Returns the current survey locale
            * @see SurveyModel.locale
            */
        getLocale(): string;
        getMarkdownHtml(text: string): string;
}

/**
  * A Model for an question that renders empty "div" tag. It used as a base class for some custom widgets
  */
export declare class QuestionEmptyModel extends Question {
    name: string;
    constructor(name: string);
    getType(): string;
}

/**
    * It is a base class for checkbox, dropdown and radiogroup questions.
    */
export declare class QuestionSelectBase extends Question {
        protected cachedValue: any;
        /**
            * Use this property to fill the choices from a restful service.
            * @see choices
            */
        choicesByUrl: ChoicesRestfull;
        choicesChangedCallback: () => void;
        constructor(name: string);
        /**
            * Returns the other item. By using this property, you may change programmatically it's value and text.
            */
        readonly otherItem: ItemValue;
        /**
            * Returns true if a user select the 'other' item.
            */
        readonly isOtherSelected: boolean;
        protected getHasOther(val: any): boolean;
        readonly validatedValue: any;
        protected createRestfull(): ChoicesRestfull;
        protected getComment(): string;
        protected setComment(newValue: string): void;
        protected setNewValue(newValue: any): void;
        protected valueFromData(val: any): any;
        protected valueToData(val: any): any;
        protected valueFromDataCore(val: any): any;
        protected valueToDataCore(val: any): any;
        protected hasUnknownValue(val: any): boolean;
        /**
            * The list of items. Every item has value and text. If text is empty, the value is rendered. The item text supports markdown.
            * @see choicesByUrl
            */
        choices: Array<any>;
        /**
            * By default the entered text in the others input in the checkbox/radiogroup/dropdown are stored as "question name " + "-Comment". The value itself is "question name": "others". Set this property to false, to store the entered text directly in the "question name" key.
            * @see SurveyModel.storeOthersAsComment
            */
        storeOthersAsComment: boolean;
        protected hasOtherChanged(): void;
        /**
            * Use this property to render items in a specific order.
            */
        choicesOrder: string;
        /**
            * Use this property to set the different text for other item.
            */
        otherText: string;
        readonly locOtherText: LocalizableString;
        /**
            * The text that shows when the other item is choosed by the other input is empty.
            */
        otherErrorText: string;
        readonly locOtherErrorText: LocalizableString;
        /**
            * The list of items as they will be rendered. If needed items are sorted and the other item is added.
            * @see hasOther
            * @see choicesOrder
            */
        readonly visibleChoices: Array<ItemValue>;
        /**
            * Returns the text for the current value. If the value is null then returns empty string. If 'other' is selected then returns the text for other value.
            */
        readonly displayValue: any;
        protected getDisplayValue(items: ItemValue[], val: any): any;
        supportComment(): boolean;
        supportOther(): boolean;
        protected onCheckForErrors(errors: Array<SurveyError>): void;
        setSurveyImpl(value: ISurveyImpl): void;
        onLocaleChanged(): void;
        protected getStoreOthersAsComment(): boolean;
        onSurveyLoad(): void;
        onAnyValueChanged(name: string): void;
        clearUnusedValues(): void;
}
/**
    * A base class for checkbox and radiogroup questions. It introduced a colCount property.
    */
export declare class QuestionCheckboxBase extends QuestionSelectBase {
        name: string;
        colCountChangedCallback: () => void;
        constructor(name: string);
        /**
            * The number of columns for radiogroup and checkbox questions. Items are rendred in one line if the value is 0.
            */
        colCount: number;
}

/**
  * A Model for a checkbox question
  */
export declare class QuestionCheckboxModel extends QuestionCheckboxBase {
    name: string;
    constructor(name: string);
    readonly displayValue: any;
    protected getHasOther(val: any): boolean;
    protected valueFromData(val: any): any;
    protected valueFromDataCore(val: any): any;
    protected valueToDataCore(val: any): any;
    getType(): string;
}

/**
    * A Model for a comment question
    */
export declare class QuestionCommentModel extends Question {
        name: string;
        constructor(name: string);
        /**
            * Use this property to set the input place holder.
            */
        placeHolder: string;
        readonly locPlaceHolder: LocalizableString;
        /**
            * The html rows attribute.
            */
        rows: number;
        /**
            * The html cols attribute.
            */
        cols: number;
        getType(): string;
        isEmpty(): boolean;
}

/**
    * A Model for a dropdown question
    */
export declare class QuestionDropdownModel extends QuestionSelectBase {
        name: string;
        constructor(name: string);
        /**
            * Use this property to set the options caption different from the default value. The default value is taken from localization strings.
            */
        optionsCaption: string;
        readonly locOptionsCaption: LocalizableString;
        getType(): string;
        supportGoNextPageAutomatic(): boolean;
}

export declare class QuestionFactory {
    static Instance: QuestionFactory;
    static readonly DefaultChoices: string[];
    static readonly DefaultColums: string[];
    static readonly DefaultRows: string[];
    registerQuestion(questionType: string, questionCreator: (name: string) => QuestionBase): void;
    clear(): void;
    getAllTypes(): Array<string>;
    createQuestion(questionType: string, name: string): QuestionBase;
}
export declare class ElementFactory {
    static Instance: ElementFactory;
    registerElement(elementType: string, elementCreator: (name: string) => IElement): void;
    clear(): void;
    getAllTypes(): Array<string>;
    createElement(elementType: string, name: string): IElement;
}

/**
    * A Model for a file question
    */
export declare class QuestionFileModel extends Question {
        name: string;
        previewValueLoadedCallback: () => void;
        constructor(name: string);
        getType(): string;
        /**
            * Set it to true, to show the preview for the image files.
            */
        showPreview: boolean;
        /**
            * The image height.
            */
        imageHeight: string;
        /**
            * The image width.
            */
        imageWidth: string;
        /**
            * Set it to false if you do not want to serialize file content as text in the survey.data.
            * In this case, you have to write the code onUploadFile event to store the file content.
            * @see SurveyModel.onUploadFile
            */
        storeDataAsText: boolean;
        /**
            * Use this property to setup the maximum allowed file size.
            */
        maxSize: number;
        /**
            * Load file programmatically.
            * @param file
            */
        loadFile(file: File): void;
        previewValue: any;
        protected setFileValue(file: File): void;
        protected onCheckForErrors(errors: Array<SurveyError>): void;
}

/**
  * A Model for html question. Unlike other questions it doesn't have value and title.
  */
export declare class QuestionHtmlModel extends QuestionBase {
    name: string;
    constructor(name: string);
    getType(): string;
    html: string;
    readonly locHtml: LocalizableString;
    readonly processedHtml: string;
}

/**
  * A Model for a radiogroup question.
  */
export declare class QuestionRadiogroupModel extends QuestionCheckboxBase {
    name: string;
    constructor(name: string);
    getType(): string;
    supportGoNextPageAutomatic(): boolean;
}

/**
    * A Model for a rating question.
    */
export declare class QuestionRatingModel extends Question {
        name: string;
        static MaximumRateValueCount: number;
        rateValuesChangedCallback: () => void;
        constructor(name: string);
        onSurveyLoad(): void;
        /**
            * The list of rate items. Every item has value and text. If text is empty, the value is rendered. The item text supports markdown. If it is empty the array is generated by using rateMin, rateMax and rateStep properties.
            * @see rateMin
            * @see rateMax
            * @see rateStep
            */
        rateValues: Array<any>;
        /**
            * This property is used to generate rate values if rateValues array is empty. It is the first value in the rating. The default value is 1.
            * @see rateValues
            * @see rateMax
            * @see rateStep
            */
        rateMin: number;
        /**
            * This property is used to generate rate values if rateValues array is empty. It is the last value in the rating. The default value is 5.
            * @see rateValues
            * @see rateMin
            * @see rateStep
            */
        rateMax: number;
        /**
            * This property is used to generate rate values if rateValues array is empty. It is the step value. The number of rate values are (rateMax - rateMin) / rateStep. The default value is 1.
            * @see rateValues
            * @see rateMin
            * @see rateMax
            */
        rateStep: number;
        readonly visibleRateValues: ItemValue[];
        getType(): string;
        supportGoNextPageAutomatic(): boolean;
        supportComment(): boolean;
        supportOther(): boolean;
        /**
            * The description of minimum (first) item.
            */
        minRateDescription: string;
        readonly locMinRateDescription: LocalizableString;
        /**
            * The description of maximum (last) item.
            */
        maxRateDescription: string;
        readonly locMaxRateDescription: LocalizableString;
}

/**
    * A Model for expression question. It is a read-only question. It calculates value based on epxression property.
    */
export declare class QuestionExpressionModel extends Question {
        name: string;
        constructor(name: string);
        getType(): string;
        /**
            * Use this property to display the value in your own format. Make sure you have "{0}" substring in your string, to display the actual value.
            */
        format: string;
        readonly locFormat: LocalizableString;
        /**
            * The Expression that used to calculate the question value. You may use standard operators like +, -, * and /, squares (). Here is the example of accessing the question value {questionname}.
            * <br/>Example: "({quantity} * {price}) * (100 - {discount}) / 100"
            */
        expression: string;
        runCondition(values: HashTable<any>): void;
        readonly displayValue: any;
        /**
            * You may set this property to "decimal", "currency" or "percent". If you set it to "currency", you may use the currency property to display the value in currency different from USD.
            * @see currency
            */
        displayStyle: string;
        /**
            * Use it to display the value in the currency differen from USD. The displayStype should be set to "currency".
            * @see displayStyle
            */
        currency: string;
        useGrouping: boolean;
        protected getValueAsStr(val: any): string;
}

/**
    * A Model for an input text question.
    */
export declare class QuestionTextModel extends Question {
        name: string;
        constructor(name: string);
        getType(): string;
        /**
            * Use this property to change the default input type.
            */
        inputType: string;
        /**
            * The text input size
            */
        size: number;
        isEmpty(): boolean;
        supportGoNextPageAutomatic(): boolean;
        /**
            * The input place holder.
            */
        placeHolder: string;
        readonly locPlaceHolder: LocalizableString;
        protected setNewValue(newValue: any): void;
        protected correctValueType(newValue: any): any;
}

/**
    * A Model for a boolean question.
    */
export declare class QuestionBooleanModel extends Question {
        name: string;
        constructor(name: string);
        getType(): string;
        /**
            * Returns true if the question check will be rendered in indeterminate mode. value is empty.
            */
        readonly isIndeterminate: boolean;
        readonly hasTitle: boolean;
        supportGoNextPageAutomatic(): boolean;
        /**
            * Get/set question value in 3 modes: indeterminate (value is empty), true (check is set) and false (check is unset).
            * @see valueTrue
            * @see valueFalse
            */
        checkedValue: any;
        /**
            * Set the default state of the check: "indeterminate" - default (value is empty/null), "true" - value equals valueTrue or true, "false" - value equals valueFalse or false.
            */
        defaultValue: any;
        /**
            * The checkbox label. If it is empty and showTitle is false then title is rendered
            * @see showTitle
            * @see title
            */
        label: string;
        readonly locLabel: LocalizableString;
        readonly locDisplayLabel: LocalizableString;
        /**
            * Set this property to true to show the question title. It is hidden by default.
            */
        showTitle: boolean;
        /**
            * Set this property, if you want to have a different value from true when check is set.
            */
        valueTrue: any;
        /**
            * Set this property, if you want to have a different value from false when check is unset.
            */
        valueFalse: any;
        protected setDefaultValue(): void;
}

export interface IQuestionPanelDynamicData {
        getItemIndex(item: QuestionPanelDynamicItem): number;
        getPanelItemData(item: QuestionPanelDynamicItem): any;
        setPanelItemData(item: QuestionPanelDynamicItem, name: string, val: any): any;
        getSurvey(): ISurvey;
}
export declare class QuestionPanelDynamicItem implements ISurveyData, ISurveyImpl, ITextProcessor {
        static ItemVariableName: string;
        static IndexVariableName: string;
        constructor(data: IQuestionPanelDynamicData, panel: PanelModel);
        readonly panel: PanelModel;
        runCondition(values: HashTable<any>): void;
        getValue(name: string): any;
        setValue(name: string, newValue: any): void;
        getComment(name: string): string;
        setComment(name: string, newValue: string): void;
        onSurveyValueChanged(): void;
        setVisibleIndex(index: number, showIndex: boolean): number;
        getAllValues(): any;
        geSurveyData(): ISurveyData;
        getSurvey(): ISurvey;
        getTextProcessor(): ITextProcessor;
        processText(text: string, returnDisplayValue: boolean): string;
        processTextEx(text: string): any;
        onAnyValueChanged(name: string): void;
}
/**
    * A Model for a panel dymanic question. You setup the template panel, but adding elements (any question or a panel) and assign a text to it's title, and this panel will be used as a template on creating dynamic panels. The number of panels is defined by panelCount property.
    * An end-user may dynamically add/remove panels, unless you forbidden this.
    */
export declare class QuestionPanelDynamicModel extends Question implements IQuestionPanelDynamicData {
        name: string;
        static MaxPanelCount: number;
        renderModeChangedCallback: () => void;
        panelCountChangedCallback: () => void;
        currentIndexChangedCallback: () => void;
        constructor(name: string);
        setSurveyImpl(value: ISurveyImpl): void;
        getType(): string;
        readonly isAllowTitleLeft: boolean;
        removeElement(element: IElement): boolean;
        /**
            * The template Panel. This panel is used as a template on creatign dynamic panels
            * @see  templateElements
            * @see templateTitle
            * @see panelCount
            */
        readonly template: PanelModel;
        /**
            * The template Panel elements, questions and panels.
            * @see  templateElements
            * @see template
            * @see panelCount
            */
        readonly templateElements: Array<IElement>;
        /**
            * The template Panel title property.
            * @see  templateElements
            * @see template
            * @see panelCount
            */
        templateTitle: string;
        readonly locTemplateTitle: LocalizableString;
        /**
            * The template Panel description property.
            * @see  templateElements
            * @see template
            * @see panelCount
            * @see templateTitle
            */
        templateDescription: string;
        readonly locTemplateDescription: LocalizableString;
        protected readonly items: Array<QuestionPanelDynamicItem>;
        /**
            * The array of dynamic panels created based on panel template
            * @see template
            * @see panelCount
            */
        readonly panels: Array<PanelModel>;
        /**
            * The index of current active dynamical panel when the renderMode is not "list". If there is no dymamic panel (panelCount = 0) or renderMode equals "list" it returns -1, otherwise it returns a value from 0 to panelCount - 1.
            * @see currentPanel
            * @see panels
            * @see panelCount
            * @see renderMode
            */
        currentIndex: number;
        /**
            * The current active dynamical panel when the renderMode is not "list". If there is no dymamic panel (panelCount = 0) or renderMode equals "list" it returns null.
            * @see currenIndex
            * @see panels
            * @see panelCount
            * @see renderMode
            */
        readonly currentPanel: PanelModel;
        /**
            * Set it to true, to show a confirmation dialog on removing a panel
            * @see ConfirmDeleteText
            */
        confirmDelete: boolean;
        /**
            * Set it to a question name used in the template panel and the library shows duplication error, if there are same values in different panels of this question.
            * @see keyDuplicationError
            */
        keyName: string;
        /**
            * Use this property to change the default text showing in the confirmation delete dialog on removing a panel.
            */
        confirmDeleteText: string;
        readonly locConfirmDeleteText: LocalizableString;
        /**
            * The duplication value error text. Set it to show the text different from the default.
            * @see keyName
            */
        keyDuplicationError: string;
        readonly locKeyDuplicationError: LocalizableString;
        /**
            * Use this property to change the default previous button text. Previous button shows the previous panel, change the currentPanel, when the renderMode doesn't equal to "list".
            * @see currentPanel
            * @see currentIndex
            * @see renderMode
            */
        panelPrevText: string;
        readonly locPanelPrevText: LocalizableString;
        /**
            * Use this property to change the default next button text. Next button shows the next panel, change the currentPanel, when the renderMode doesn't equal to "list".
            * @see currentPanel
            * @see currentIndex
            * @see renderMode
            */
        panelNextText: string;
        readonly locPanelNextText: LocalizableString;
        /**
            * Use this property to change the default value of add panel button text.
            */
        panelAddText: string;
        readonly locPanelAddText: LocalizableString;
        /**
            * Use this property to change the default value of remove panel button text.
            */
        panelRemoveText: string;
        readonly locPanelRemoveText: LocalizableString;
        /**
            * Returns true when the renderMode equals to "progressTop" or "progressTopBottom"
            */
        readonly isProgressTopShowing: boolean;
        /**
            * Returns true when the renderMode equals to "progressBottom" or "progressTopBottom"
            */
        readonly isProgressBottomShowing: boolean;
        /**
            * Returns true when currentIndex is more than 0.
            * @see currenIndex
            * @see currenPanel
            */
        readonly isPrevButtonShowing: boolean;
        /**
            * Returns true when currentIndex is more than or equal 0 and less then panelCount - 1.
            * @see currenIndex
            * @see currenPanel
            * @see panelCount
            */
        readonly isNextButtonShowing: boolean;
        /**
            * Returns true when showRangeInProgress equals to true, renderMode doesn't equal to "list" and panelCount is >= 2.
            */
        readonly isRangeShowing: boolean;
        getElementsInDesign(includeHidden?: boolean): Array<IElement>;
        /**
            * Use this property to get/set the number of dynamic panels.
            * @see template
            * @see minPanelCount
            * @see maxPanelCount
            * @see addPanel
            * @see removePanel
            * @see removePanelUI
            */
        panelCount: number;
        /**
            * The minimum panel count. A user could not delete a panel if the panelCount equals to minPanelCount
            * @see panelCount
            * @see maxPanelCount
            */
        minPanelCount: number;
        /**
            * The maximum panel count. A user could not add a panel if the panelCount equals to maxPanelCount
            * @see panelCount
            * @see minPanelCount
            */
        maxPanelCount: number;
        /**
            * Set this property to false to hide the 'Add New' button
            * @see allowRemovePanel
            */
        allowAddPanel: boolean;
        /**
            * Set this property to false to hide the 'Remove' button
            * @see allowAddPanel
            */
        allowRemovePanel: boolean;
        /**
            * Set this property different from "default" to set the specific question title location for the template questions.
            * @see SurveyModel.questionTitleLocation
            * @see PanelModelBase.questionTitleLocation
            */
        templateTitleLocation: string;
        /**
            * Use this property to show/hide the numbers in titles in questions inside a dynamic panel.
            * By default the value is "off". You may set it to "onPanel" and the first question inside a dynamic panel will start with 1 or "onSurvey" to include nested questions in dymamic panels into global survey question numbering.
            */
        showQuestionNumbers: string;
        /**
            * Shows the range from 1 to panelCount when renderMode doesn't equal to "list". Set to false to hide this element.
            * @see panelCount
            * @see renderMode
            */
        showRangeInProgress: boolean;
        /**
            * By default the property equals to "list" and all dynamic panels are rendered one by one on the page. You may change it to: "progressTop", "progressBottom" or "progressTopBottom" to render only one dynamic panel at once. The progress and navigation elements can be rendred on top, bottom or both.
            */
        renderMode: string;
        /**
            * Returns true when renderMode equals to "list".
            * @renderMode
            */
        readonly isRenderModeList: boolean;
        setVisibleIndex(value: number): number;
        /**
            * Returns true when an end user may add a new panel. The question is not read only and panelCount less than maxPanelCount
            * @see isReadOnly
            * @see panelCount
            * @see maxPanelCount
            */
        readonly canAddPanel: boolean;
        /**
            * Returns true when an end user may remove a panel. The question is not read only and panelCount is more than minPanelCount
            * @see isReadOnly
            * @see panelCount
            * @see minPanelCount
            */
        readonly canRemovePanel: boolean;
        protected rebuildPanels(): void;
        /**
            * Add a new dynamic panel based on the template Panel. It checks if canAddPanel returns true and then calls addPanel method.
            * @see template
            * @see panelCount
            * @see panels
            * @see canAddPanel
            */
        addPanelUI(): PanelModel;
        /**
            * Add a new dynamic panel based on the template Panel.
            * @see template
            * @see panelCount
            * @see panels
            */
        addPanel(): PanelModel;
        /**
            * Call removePanel function. Do nothing is canRemovePanel returns false. If confirmDelete set to true, it shows the confirmation dialog first.
            * @param value a panel or panel index
            * @see removePanel
            * @see confirmDelete
            * @see confirmDeleteText
            * @see canRemovePanel
            *
            */
        removePanelUI(value: any): void;
        /**
            * Removes a dynamic panel from the panels array.
            * @param value a panel or panel index
            * @see panels
            * @see template
            */
        removePanel(value: any): void;
        onSurveyLoad(): void;
        runCondition(values: HashTable<any>): void;
        protected runPanelsCondition(values: HashTable<any>): void;
        onAnyValueChanged(name: string): void;
        hasErrors(fireCallback?: boolean): boolean;
        clearValueIfInvisible(): void;
        getAllErrors(): Array<SurveyError>;
        protected createNewItem(): QuestionPanelDynamicItem;
        protected createNewPanel(): PanelModel;
        protected createAndSetupNewPanelObject(): PanelModel;
        protected createNewPanelObject(): PanelModel;
        protected onValueChanged(): void;
        onSurveyValueChanged(newValue: any): void;
        protected onSetData(): void;
        getItemIndex(item: QuestionPanelDynamicItem): number;
        getPanelItemData(item: QuestionPanelDynamicItem): any;
        setPanelItemData(item: QuestionPanelDynamicItem, name: string, val: any): void;
        getSurvey(): ISurvey;
}

export declare var surveyTimerFunctions: {
    setInterval: (func: () => any) => number;
    clearInterval: (timerId: number) => void;
};
export declare class SurveyTimer {
    static readonly instance: SurveyTimer;
    onTimer: Event<() => any, any>;
    start(func?: () => any): void;
    stop(func?: () => any): void;
    doTimer(): void;
}

/**
    * Survey object contains information about the survey. Pages, Questions, flow logic and etc.
    */
export declare class SurveyModel extends Base implements ISurvey, ISurveyData, ISurveyImpl, ISurveyTriggerOwner, ILocalizableOwner {
        static platform: string;
        readonly platformName: string;
        /**
            * You may show comments input for the most of questions. The entered text in the comment input will be saved as 'question name' + 'commentPrefix'.
            * @see data
            */
        commentPrefix: string;
        /**
            * The event is fired after a user click on 'Complete' button and finished the survey. You may use it to send the data to your web server.
            * <br/> sender the survey object that fires the event
            * <br/> options.showDataSaving(text) call this method to show that the survey is saving the data on your server. The text is an optional parameter to show your message instead of default.
            * <br/> options.showDataSavingError(text) call this method to show that there is an error on saving the data on your server. If you want to show a custom error, use an optional text parameter.
            * <br/> options.showDataSavingSuccess(text) call this method to show that the data were successful saved on the server.
            * <br/> options.showDataSavingClear call this method to hide the text about the saving progress.
            * @see data
            * @see clearInvisibleValues
            * @see completeLastPage
            * @see surveyPostId
            */
        onComplete: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired after the survey changed it's state from "starting" to "running". The "starting" state means that survey shows the started page.
            * The firstPageIsStarted property should be set to the true, if you want to have the started page in your survey. The end-user should click on the "Start" button to start the survey.
            * @see firstPageIsStarted
            */
        onStarted: Event<(sender: SurveyModel) => any, any>;
        /**
            * The event is fired on clicking 'Next' page if sendResultOnPageNext is set to true. You may use it to save the intermediate results, for example, if your survey is large enough.
            * <br/> sender the survey object that fires the event
            * @see sendResultOnPageNext
            */
        onPartialSend: Event<(sender: SurveyModel) => any, any>;
        /**
            * The event is fired when another page becomes the current. Typically it happens when a user click on 'Next' or 'Prev' buttons.
            * <br/> sender the survey object that fires the event
            * <br/> option.oldCurrentPage the previous current/active page
            * <br/> option.newCurrentPage a new current/active page
            * @see currentPage
            * @see currentPageNo
            * @see nextPage
            * @see prevPage
            * @see completeLastPage
            */
        onCurrentPageChanged: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired when the question value is changed. It can be done via UI by a user or programmatically on calling setValue method.
            * <br/> sender the survey object that fires the event
            * <br/> options.name the value name that has been changed
            * <br/> options.question a question which question.name equals to the value name. If there are several questions with the same name, the first question is taken. If there is no such questions, the options.question is null.
            * <br/> options.value a new value
            * @see setValue
            */
        onValueChanged: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on changing a question visibility.
            * <br/> sender the survey object that fires the event
            * <br/> options.question a question which visibility has been changed
            * <br/> options.name a question name
            * <br/> options.visible a question visible boolean value
            * @see QuestionBase.visibile
            * @see QuestionBase.visibileIf
            */
        onVisibleChanged: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on changing a page visibility.
            * <br/> sender the survey object that fires the event
            * <br/> options.page a page  which visibility has been changed
            * <br/> options.visible a page visible boolean value
            * @see PageModel.visibile
            * @see PageModel.visibileIf
            */
        onPageVisibleChanged: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on changing a panel visibility.
            * <br/> sender the survey object that fires the event
            * <br/> options.panel a panel which visibility has been changed
            * <br/> options.visible a panel visible boolean value
            * @see PanelModel.visibile
            * @see PanelModel.visibileIf
            */
        onPanelVisibleChanged: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on adding a new question into survey.
            * 'question': question, 'name': question.name, 'index': index, 'parentPanel': parentPanel, 'rootPanel': rootPanel
            * <br/> sender the survey object that fires the event
            * <br/> options.question a newly added question object.
            * <br/> options.name a question name
            * <br/> options.index a index of the question in the container (page or panel)
            * <br/> options.parentPanel a container where question is located. It can be page or panel.
            * <br/> options.rootPanel typically it is a page.
            * @see QuestionBase
            */
        onQuestionAdded: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on removing a question from survey
            * <br/> sender the survey object that fires the event
            * <br/> options.question a removed question object.
            * <br/> options.name a question name
            * @see QuestionBase
            */
        onQuestionRemoved: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on adding a panel into survey
            * <br/> sender the survey object that fires the event
            * <br/> options.panel a newly added panel object.
            * <br/> options.name a panel name
            * <br/> options.index a index of the panel in the container (page or panel)
            * <br/> options.parentPanel a container where question is located. It can be page or panel.
            * <br/> options.rootPanel typically it is a page.
            * @see PanelModel
            */
        onPanelAdded: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on removing a panel from survey
            * <br/> sender the survey object that fires the event
            * <br/> options.panel a removed panel object.
            * <br/> options.name a panel name
            * @see PanelModel
            */
        onPanelRemoved: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on adding a page into survey
            * <br/> sender the survey object that fires the event
            * <br/> options.page a newly added panel object.
            * @see PanelModel
            */
        onPageAdded: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on validating value in a question. Set your error to options.error and survey will show the error for the question and block completing the survey or going to the next page.
            * <br/> sender the survey object that fires the event
            * <br/> options.question a question
            * <br/> options.name a question name
            * <br/> options.value the current question value
            * <br/> options.error an error string. It is empty by default.
            * @see onServerValidateQuestions
            */
        onValidateQuestion: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * Use this event to validate data on your server.
            * <br/> sender the survey object that fires the event
            * <br/> options.data the values of all non-empty questions on the current page. You can get a question value as options.data["myQuestionName"].
            * <br/> options.errors set your errors to this object as: options.errors["myQuestionName"] = "Error text";. It will be shown as a question error.
            * @see onValidateQuestion
            * @see onValidatePanel
            */
        onServerValidateQuestions: (sender: SurveyModel, options: any) => any;
        /**
            * The event is fired on validating a panel. Set your error to options.error and survey will show the error for the panel and block completing the survey or going to the next page.
            * <br/> sender the survey object that fires the event
            * <br/> options.name a panel name
            * <br/> options.error an error string. It is empty by default.
            * @see onValidateQuestion
            */
        onValidatePanel: Event<(sender: SurveyModel, options: any) => any, any>;
        onProcessHtml: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * Use this event to process the markdown text.
            * <br/> sender the survey object that fires the event
            * <br/> options.text a text that is going to be rendered
            * <br/> options.html a html. It is null by default. Set it and survey will use it instead of options.text
            */
        onTextMarkdown: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event fires when it get response from the [dxsurvey.com](http://www.dxsurvey.com) service on saving survey results. Use it to find out if the results have been saved successful.
            * <br/> sender the survey object that fires the event
            * <br/> options.success it is true if the results were sent to the service successful
            * <br/> options.response a response from the service
            */
        onSendResult: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * Use it to get results after calling the getResult method. It returns a simple analytic from [dxsurvey.com](http://www.dxsurvey.com) service.
            * <br/> sender the survey object that fires the event
            * <br/> options.success it is true if the results were got from the service successful
            * <br/> options.data the object {AnswersCount, QuestionResult : {} }. AnswersCount is the number of posted survey results. QuestionResult is an object with all possible unique answers to the question and number of these answers.
            * <br/> options.dataList an array of objects {name, value}, where 'name' is an unique value/answer to the question and value is a number/count of such answers.
            * <br/> options.response the server response
            * @see getResult
            */
        onGetResult: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on uploading the file in QuestionFile. You may use it to change the file name or tells the library do not accept the file. There are three properties in options: options.name, options.file and options.accept.
            * <br/> sender the survey object that fires the event
            * name: name, file: file, accept: accept
            * <br/> name the file name
            * <br/> file the Javascript File object
            * <br/> accept a boolean value, true by default. Set it to false to deny this file to upload
            * @see uploadFile
            */
        onUploadFile: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired before rendering a question. Use it to override the default question css classes.
            * There are two parameters in options: options.question and options.cssClasses
            * <br/> sender the survey object that fires the event
            * <br/> options.question a question for which you may change the css classes
            * <br/> options.cssClasses an object with css classes. For example {root: "table", button: "button"}. You may change them to your own css classes.
            */
        onUpdateQuestionCssClasses: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired before rendering a panel or page. Use it to override the default panel/page css classes.
            * There are two parameters in options: options.panel and options.cssClasses
            * <br/> sender the survey object that fires the event
            * <br/> options.panel a panel for which you may change the css classes
            * <br/> options.cssClasses an object with css classes. For example {title: "sv_p_title", description: "small"}. You may change them to your own css classes.
            */
        onUpdatePanelCssClasses: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired right after survey is rendered in DOM. options.htmlElement is the root element.
            * <br/> sender the survey object that fires the event
            * <br/> options.htmlElement a root html element binded with the survey object
            */
        onAfterRenderSurvey: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired right after a page is rendred in DOM. Use it to modify html elements. There are two parameters in options: options.currentPage, options.htmlElement
            * <br/> sender the survey object that fires the event
            * <br/> options.page a page object for which the event is fired. Typically the current/active page.
            * <br/> options.htmlElement an html element binded with the page object
            */
        onAfterRenderPage: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired right after a question is rendred in DOM. Use it to modify html elements. There are two parameters in options: options.question, options.htmlElement
            * <br/> sender the survey object that fires the event
            * <br/> options.question a question object for which the event is fired
            * <br/> options.htmlElement an html element binded with the question object
            */
        onAfterRenderQuestion: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired right after a panel is rendred in DOM. Use it to modify html elements. There are two parameters in options: options.panel, options.htmlElement
            * <br/> sender the survey object that fires the event
            * <br/> options.panel a panel object for which the event is fired
            * <br/> options.htmlElement an html element binded with the panel object
            */
        onAfterRenderPanel: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on adding a new row in Matrix Dynamic question.
            * <br/> sender the survey object that fires the event
            * <br/> options.question a matrix question.
            * @see QuestionMatrixDynamicModel
            * @see QuestionMatrixDynamicModel.visibleRows
            */
        onMatrixRowAdded: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on removing a row from Matrix Dynamic question.
            * <br/> sender the survey object that fires the event
            * <br/> options.question a matrix question.
            * <br/> options.rowIndex a removed row index.
            * <br/> options.row a removed row object.
            * @see QuestionMatrixDynamicModel
            * @see QuestionMatrixDynamicModel.visibleRows
            */
        onMatrixRowRemoved: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired for every cell created in Matrix Dymic and Matrix Dropdown questions.
            * <br/> options.question - the matrix question
            * <br/> options.cell - the matrix cell
            * <br/> options.cellQuestion - the question/editor in the cell. You may customize it, change it's properties, like choices or visible.
            * <br/> options.rowValue - the value of the current row. To access the value of paticular column use: options.rowValue["columnValue"]
            * <br/> options.column - the matrix column object
            * <br/> options.columName - the matrix column name
            * <br/> options.row - the matrix row object
            * @see onMatrixRowAdded
            * @see QuestionMatrixDynamicModel
            * @see QuestionMatrixDropdownModel
            */
        onMatrixCellCreated: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired for every cell after is has been rendered in DOM.
            * <br/> options.question - the matrix question
            * <br/> options.cell - the matrix cell
            * <br/> options.cellQuestion - the question/editor in the cell.
            * <br/> options.htmlElement a html element binded with the cellQuestion object
            * <br/> options.column - the matrix column object
            * <br/> options.row - the matrix row object
            * @see onMatrixCellCreated
            * @see QuestionMatrixDynamicModel
            * @see QuestionMatrixDropdownModel
            */
        onMatrixAfterCellRender: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired when cell value is changed in Matrix Dymic and Matrix Dropdown questions.
            * <br/> options.question - the matrix question
            * <br/> options.columName - the matrix column name
            * <br/> options.value - a new value
            * <br/> options.row - the matrix row object
            * <br/> options.getCellQuestion(columnName) - the function that returns the cell question by column name.
            * @see onMatrixRowAdded
            * @see QuestionMatrixDynamicModel
            * @see QuestionMatrixDropdownModel
            */
        onMatrixCellValueChanged: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired when Matrix Dymic and Matrix Dropdown questions validate the cell value.
            * <br/> options.question - the matrix question
            * <br/> options.columName - the matrix column name
            * <br/> options.value - a cell value
            * <br/> options.row - the matrix row object
            * <br/> options.getCellQuestion(columnName) - the function that returns the cell question by column name.
            * @see onMatrixRowAdded
            * @see QuestionMatrixDynamicModel
            * @see QuestionMatrixDropdownModel
            */
        onMatrixCellValidate: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on adding a new panel in Panel Dynamic question.
            * <br/> sender the survey object that fires the event
            * <br/> options.question a panel question.
            * @see QuestionPanelDynamicModel
            * @see QuestionPanelDynamicModel.panels
            */
        onDynamicPanelAdded: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired on removing a panel from Panel Dynamic question.
            * <br/> sender the survey object that fires the event
            * <br/> options.question a panel question.
            * <br/> options.panelIndex a removed panel index.
            * @see QuestionPanelDynamicModel
            * @see QuestionPanelDynamicModel.panels
            */
        onDynamicPanelRemoved: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The event is fired every second if the method startTimer has been called.
            * @see startTimer
            * @see timeSpent
            * @see Page.timeSpent
            */
        onTimer: Event<(sender: SurveyModel) => any, any>;
        /**
            * The event is fired before displaying a new information in the Timer Panel. Use it to change the default text.
            * <br/> options.text - the timer panel info text.
            */
        onTimerPanelInfoText: Event<(sender: SurveyModel, options: any) => any, any>;
        /**
            * The list of errors on loading survey json. If the list is empty after loading a json then the json is correct and there is no errors in it.
            * @see JsonError
            */
        jsonErrors: Array<JsonError>;
        constructor(jsonObj?: any);
        getType(): string;
        /**
            * The list of all pages in the survey, including invisible.
            * @see PageModel
            * @see visiblePages
            */
        readonly pages: Array<PageModel>;
        /**
            * The list of triggers in the survey.
            * @see SurveyTrigger
            */
        triggers: Array<SurveyTrigger>;
        /**
            * Set this property to automatically load survey Json from [dxsurvey.com](http://www.dxsurvey.com) service.
            * @see loadSurveyFromService
            */
        surveyId: string;
        /**
            * Set this property to automatically save the data into the [dxsurvey.com](http://www.dxsurvey.com) service.
            * @see onComplete
            * @see surveyShowDataSaving
            */
        surveyPostId: string;
        /**
            * Use this property as indentificator for a user, for example e-mail or unique customer id in your web application. If you are loading survey or posting survey results  from/to [dxsurvey.com](http://www.dxsurvey.com) service, then the library do not allow to run the same survey the second time. On the second run, the user will see the 'Thank you' page.
            */
        clientId: string;
        /**
            * If the property is not empty, before starting to run the survey, the library checkes if the cookie with this name exists. If it is true, the survey goes to complete mode and an user sees the 'Thank you' page. On completing the survey the cookie with this name is created.
            */
        cookieName: string;
        /**
            * Set it to true, to save results on completing every page. onPartialSend event is fired.
            * @see onPartialSend
            * @see clientId
            */
        sendResultOnPageNext: boolean;
        /**
            * Set this property to true, to show the progress on saving/sending data into the [dxsurvey.com](http://www.dxsurvey.com) service.
            * @see surveyPostId
            */
        surveyShowDataSaving: boolean;
        /**
            * On showing the next or previous page, a first input is focused, if the property set to true.
            */
        focusFirstQuestionAutomatic: boolean;
        /**
            * Set it to false to hide 'Prev', 'Next' and 'Complete' buttons. It makes sense if you are going to create a custom navigation or have just one page or on setting goNextPageAutomatic property.
            * @see goNextPageAutomatic
            * @see showPrevButton
            */
        showNavigationButtons: boolean;
        /**
            * Set it to false to hide the 'Prev' to disable for end-users go back to their answers.
            * @see showNavigationButtons
            */
        showPrevButton: boolean;
        /**
            * Set it to false hide survey title.
            * @see title
            */
        showTitle: boolean;
        /**
            * Set it to false to hide page titles.
            * @see PageModel.title
            */
        showPageTitles: boolean;
        /**
            * On finishing the survey the 'Thank you', page on complete, is shown. Set the property to false, to hide the 'Thank you' page.
            * @see data
            * @see onComplete
            */
        showCompletedPage: boolean;
        /**
            * A char/string that will be rendered in the title required questions.
            * @see QuestionBase.title
            */
        requiredText: string;
        /**
            * By default the first question index is 1. You may start it from 100 or from 'A', by setting 100 or 'A' to this property.
            * @see QuestionBase.title
            * @see requiredText
            */
        questionStartIndex: string;
        /**
            * By default the entered text in the others input in the checkbox/radiogroup/dropdown are stored as "question name " + "-Comment". The value itself is "question name": "others". Set this property to false, to store the entered text directly in the "question name" key.
            * @see commentPrefix
            */
        storeOthersAsComment: boolean;
        /**
            * Set it to the one of the following constants if you want to go to the next page without pressing 'Next' button when all questions are anwered.
            * true - go next page and submit automatically
            * "autogonext" - go next page automatically but do not submit
            * false - do not go next page and not submit automatically
            * @see showNavigationButtons
            */
        goNextPageAutomatic: boolean | "autogonext";
        /**
            * Set it to 'none' to include the invisible values into the survey data.
            * </br> Set it to 'onHidden' to clear the question value when it becomes invisible.
            * </br> Leave it equals to 'onComplete', to remove from data property values of invisible questions on survey complete. In this case, the invisible questions will not be stored on the server.
            * </br> The default value is 'onComplete'.
            * @see QuestionBase.visible
            * @see onComplete
            */
        clearInvisibleValues: any;
        /**
            * Use it to change the survey locale. By default it is empty, 'en'. You may set it to 'de' - german, 'fr' - french and so on. The library has built-in localization for several languages. The library has a multi-language support as well.
            */
        locale: string;
        getLocale(): string;
        getMarkdownHtml(text: string): any;
        getLocString(str: string): any;
        /**
            * Returns the text that renders when there is no any visible page and question.
            */
        readonly emptySurveyText: string;
        /**
            * Survey title.
            */
        title: string;
        readonly locTitle: LocalizableString;
        /**
            * The html that shows on completed ('Thank you') page. Set it to change the default text.
            * @see showCompletedPage
            * @see locale
            */
        completedHtml: string;
        readonly locCompletedHtml: LocalizableString;
        /**
            * The html that shows if the end user has already completed the survey.
            * @see clientId
            * @see locale
            */
        completedBeforeHtml: string;
        readonly locCompletedBeforeHtml: LocalizableString;
        /**
            * The html that shows on loading survey Json from the dxsurvey.com service.
            * @see surveyId
            * @see locale
            */
        loadingHtml: string;
        readonly locLoadingHtml: LocalizableString;
        /**
            * A text that renders on the 'Start' button. Set it to change the default text.
            * The start button is shown on the started page. You have to set firstPageIsStarted property to true, to have the started page.
            * @see firstPageIsStarted
            * @see locale
            */
        startSurveyText: string;
        readonly locStartSurveyText: LocalizableString;
        /**
            * A text that renders on the 'Prev' button. Set it to change the default text.
            * @see locale
            */
        pagePrevText: string;
        readonly locPagePrevText: LocalizableString;
        /**
            * A text that renders on the 'Next' button. Set it to change the default text.
            * @see locale
            */
        pageNextText: string;
        readonly locPageNextText: LocalizableString;
        /**
            * A text that renders on the 'Complete' button. Set it to change the default text.
            * @see locale
            */
        completeText: string;
        readonly locCompleteText: LocalizableString;
        /**
            * A template for a question title.
            * @see QuestionModel.title
            */
        questionTitleTemplate: string;
        /**
            * Returns the question title template
            * @see questionTitleTemplate
            * @see QuestionModel.title
            */
        getQuestionTitleTemplate(): string;
        readonly locQuestionTitleTemplate: LocalizableString;
        /**
            * Set this property to false to turn off the numbering on pages titles.
            */
        showPageNumbers: boolean;
        /**
            * Set this property to "off" to turn off the numbering on questions titles or "onpage" to start numbering on every page. The default value is "on".
            */
        showQuestionNumbers: string;
        /**
            * Set this property to "top" to show the progress bar on the bottom or to "bottom" to show it on the bottom.
            */
        showProgressBar: string;
        /**
            * Returns the text/html that renders as survey title.
            */
        readonly processedTitle: string;
        /**
            * Set this property to 'bottom' or 'left' to show question title under the question or on the left.
            * <br/><b>Note:</b> Some questions, for example matrixes, do not support 'left' value. The title for them will be displayed on the top.
            */
        questionTitleLocation: string;
        /**
            * Set this property to 'bottom' to show question error(s) under the question.
            */
        questionErrorLocation: string;
        /**
            * Set this mode to 'display' to make the survey read-only. The default value is 'edit'.
            */
        mode: string;
        /**
            * An object that stores the survey results/data. You may set it directly as { 'question name': questionValue, ... }
            * @see setValue
            * @see getValue
            */
        data: any;
        getAllValues(): any;
        protected setDataValueCore(valuesHash: any, key: string, value: any): void;
        /**
            * Returns all comments from the data.
            * @see data
            */
        readonly comments: any;
        /**
            * Returns the list of visible pages. If all pages are visible then it is the same as pages property.
            * @see pages
            * @see PageModel.visible
            * @see PageModel.visibleIf
            */
        readonly visiblePages: Array<PageModel>;
        /**
            * Returns true if there is no any page in the survey. The survey is empty.
            */
        readonly isEmpty: boolean;
        /**
            * depricated, misspelling, use pageCount property
            */
        readonly PageCount: number;
        /**
            * Returns the survey pages count.
            * @see visiblePageCount
            * @see pages
            */
        readonly pageCount: number;
        /**
            * Returns the survey visible pages count
            * @see pageCount
            * @see visiblePages
            */
        readonly visiblePageCount: number;
        /**
            * Returns the started Page. firstPageIsStarted property should be equals to true
            * @see firstPageIsStarted
            */
        readonly startedPage: PageModel;
        /**
            * Returns the current survey page. If survey is rendred then it is a page that a user can see/edit.
            */
        currentPage: PageModel;
        /**
            * The index of the current page in the visible pages array. It starts from 0.
            */
        currentPageNo: number;
        /**
            * Use this property to randomize questions. Set it to 'random' to randomize questions, 'initial' to keep them in the same order. You can randomize questions on a specific page.
            * @see SurveyPage.questionsOrder
            */
        questionsOrder: string;
        /**
            * Set the input focus to the first question with the input.
            */
        focusFirstQuestion(): void;
        scrollToTopOnPageChange(): void;
        /**
            * Returns the current survey state: 'loading' - loading from the json, 'completed' - a user has completed the survey,
            * 'starting' - the started page is showing, running' - a user answers a questions right now, 'empty' - there is nothing to show in the current survey.
            */
        readonly state: string;
        readonly completedState: string;
        readonly completedStateText: string;
        protected setCompletedState(value: string, text: string): void;
        /**
            * Clear the survey data and state. If the survey has a 'completed' state, it will have a 'running' state.
            * @param clearData clear the data
            * @param gotoFirstPage make the first page as a current page.
            * @see data
            * @see state
            * @see currentPage
            */
        clear(clearData?: boolean, gotoFirstPage?: boolean): void;
        mergeValues(src: any, dest: any): void;
        protected updateCustomWidgets(page: PageModel): void;
        protected currentPageChanged(newValue: PageModel, oldValue: PageModel): void;
        /**
            * Returns the progress that a user made by answering on the survey.
            */
        getProgress(): number;
        /**
            * Returns true if navigation buttons: 'Prev', 'Next' or 'Complete' are shown.
            */
        readonly isNavigationButtonsShowing: boolean;
        /**
            * Returns true if the survey in the edit mode.
            * @see mode
            */
        readonly isEditMode: boolean;
        /**
            * Returns true if the survey in the display mode.
            * @see mode
            */
        readonly isDisplayMode: boolean;
        /**
            * Returns true if the survey in the design mode. It is used by SurveyJS Editor
            * @see setDesignMode
            */
        readonly isDesignMode: boolean;
        /**
            * Call it to set the survey into the design mode.
            * @param value use true to set the survey into the design mode.
            */
        setDesignMode(value: boolean): void;
        /**
            * Returns true, if a user has already completed the survey on this browser and there is a cookie about it. Survey goes to 'completed' state if the function returns true.
            * @see cookieName
            * @see setCookie
            * @see deleteCookie
            * @see state
            */
        readonly hasCookie: boolean;
        /**
            * Set the cookie with cookieName in the browser. It is done automatically on survey complete if cookieName is not empty.
            * @see cookieName
            * @see hasCookie
            * @see deleteCookie
            */
        setCookie(): void;
        /**
            * Delete the cookie with cookieName in the browser.
            * @see cookieName
            * @see hasCookie
            * @see setCookie
            */
        deleteCookie(): void;
        /**
            * Call it to go to the next page. It returns false, if it is the last page. If there is an error, for example required question is empty, the function returns false as well.
            * @see isCurrentPageHasErrors
            * @see prevPage
            * @see completeLastPage
            */
        nextPage(): boolean;
        /**
            * Returns true, if there is any error on the current page. For example, the required question is empty or a question validation is failed.
            * @see nextPage
            */
        readonly isCurrentPageHasErrors: boolean;
        /**
            * Call it to go to the previous page. It returns false if the current page is the first page already. It doesn't perform any checks, required questions can be empty.
            * @see isFirstPage
            */
        prevPage(): boolean;
        /**
            * Call it to complete the survey, if the current page is the last one. It returns false if there is an error on the page. If there is no errors on the page, it calls doComplete and returns true.
            * @see isCurrentPageHasErrors
            * @see nextPage
            * @see doComplete
            */
        completeLastPage(): boolean;
        /**
            * Set this property to true, if you want to combine all your pages in one page. Pages will be converted into panels.
            */
        isSinglePage: boolean;
        /**
            * Set this property to true, to make the first page your starting page. The end-user could not comeback to the start page and it is not count in the progress.
            */
        firstPageIsStarted: boolean;
        isPageStarted(page: IPage): boolean;
        protected onFirstPageIsStartedChanged(): void;
        origionalPages: any;
        protected onIsSinglePageChanged(): void;
        /**
            * Returns true if the current page is the first one.
            */
        readonly isFirstPage: boolean;
        readonly isShowPrevButton: boolean;
        /**
            * Returns true if the current page is the last one.
            */
        readonly isLastPage: boolean;
        /**
            * Call it to complete the survey. It writes cookie if cookieName property is not empty, set the survey into 'completed' state, fire onComplete event and sendResult into [dxsurvey.com](http://www.dxsurvey.com) service if surveyPostId property is not empty. It doesn't perform any validation, unlike completeLastPage function.
            * @see cookieName
            * @see state
            * @see onComplete
            * @see surveyPostId
            * @see completeLastPage
            */
        doComplete(): void;
        /**
            * Start the survey. Change the mode from "starting" to "running". You need to call it, if there is a started page in your survey, otherwise it does nothing.
            * @see firstPageIsStarted
            */
        start(): void;
        /**
            * Returns true, if at the current moment the question values on the current page are validating on the server.
            * @see onServerValidateQuestions
            */
        readonly isValidatingOnServer: boolean;
        protected onIsValidatingOnServerChanged(): void;
        protected doServerValidation(): boolean;
        protected doNextPage(): void;
        protected setCompleted(): void;
        /**
            * Returns the html for completed 'Thank you' page.
            * @see completedHtml
            */
        readonly processedCompletedHtml: string;
        /**
            * Returns the html showing that the user has already completed the survey
            * @see completedHtml
            */
        readonly processedCompletedBeforeHtml: string;
        /**
            * Returns the html that shows on loading the json.
            */
        readonly processedLoadingHtml: string;
        /**
            * Returns the text for the current progress.
            */
        readonly progressText: string;
        protected afterRenderSurvey(htmlElement: any): void;
        updateQuestionCssClasses(question: IQuestion, cssClasses: any): void;
        updatePanelCssClasses(panel: IPanel, cssClasses: any): void;
        afterRenderPage(htmlElement: any): void;
        afterRenderQuestion(question: IQuestion, htmlElement: any): void;
        afterRenderPanel(panel: IElement, htmlElement: any): void;
        matrixRowAdded(question: IQuestion): void;
        matrixRowRemoved(question: IQuestion, rowIndex: number, row: any): void;
        matrixCellCreated(question: IQuestion, options: any): void;
        matrixAfterCellRender(question: IQuestion, options: any): void;
        matrixCellValueChanged(question: IQuestion, options: any): void;
        matrixCellValidate(question: IQuestion, options: any): SurveyError;
        dynamicPanelAdded(question: IQuestion): void;
        dynamicPanelRemoved(question: IQuestion, panelIndex: number): void;
        /**
            * Upload the file into servey
            * @param name question name
            * @param file uploading file
            * @param storeDataAsText set it to true to encode file content into the survey results
            * @param uploadingCallback a call back function to get the status on uploading the file
            */
        uploadFile(name: string, file: File, storeDataAsText: boolean, uploadingCallback: (status: string) => any): boolean;
        protected createSurveyService(): dxSurveyService;
        protected uploadFileCore(name: string, file: File, uploadingCallback: (status: string) => any): void;
        getPage(index: number): PageModel;
        /**
            * Add a page into the survey
            * @param page
            * @see addNewPage
            */
        addPage(page: PageModel): void;
        /**
            * Creates a new page and adds it into the survey. Genarates a new name if the name parameter is not set.
            * @param name a page name
            * @see addPage
            */
        addNewPage(name?: string): PageModel;
        /**
            * Remove the page from the survey
            * @param page
            */
        removePage(page: PageModel): void;
        /**
            * Returns a question by its name
            * @param name a question name
            * @param caseInsensitive
            * @see getQuestionByValueName
            */
        getQuestionByName(name: string, caseInsensitive?: boolean): IQuestion;
        /**
            * Returns a question by its value name
            * @param valueName a question name
            * @param caseInsensitive
            * @see getQuestionByName
            * @see Question.valueName
            */
        getQuestionByValueName(valueName: string, caseInsensitive?: boolean): IQuestion;
        /**
            * Get a list of questions by their names
            * @param names the array of names
            * @param caseInsensitive
            */
        getQuestionsByNames(names: string[], caseInsensitive?: boolean): IQuestion[];
        /**
            * Returns a page on which an element (question or panel) is placed.
            * @param element Question or Panel
            */
        getPageByElement(element: IElement): PageModel;
        /**
            * Returns a page on which a question is located
            * @param question
            */
        getPageByQuestion(question: IQuestion): PageModel;
        /**
            * Returns a page by it's name.
            * @param name
            */
        getPageByName(name: string): PageModel;
        /**
            * Rertuns a list of pages by their names
            * @param names a list of pages names
            */
        getPagesByNames(names: string[]): PageModel[];
        /**
            * Returns the list of all questions in the survey
            * @param visibleOnly set it true, if you want to get only visible questions
            */
        getAllQuestions(visibleOnly?: boolean, includingDesignTime?: boolean): Array<IQuestion>;
        /**
            * Returns quiz questions. All visible questions that has input(s) widgets.
            */
        getQuizQuestions(): Array<IQuestion>;
        /**
            * Returns the list of all panels in the survey
            */
        getAllPanels(visibleOnly?: boolean, includingDesignTime?: boolean): Array<IPanel>;
        protected createNewPage(name: string): PageModel;
        protected notifyQuestionOnValueChanged(valueName: string, newValue: any): void;
        protected doSurveyValueChanged(question: IQuestion, newValue: any): void;
        /**
            * Send the survey result into [dxsurvey.com](http://www.dxsurvey.com) service.
            * @param postId [dxsurvey.com](http://www.dxsurvey.com) service postId
            * @param clientId Typically a customer e-mail or an identificator
            * @param isPartialCompleted Set it to true if the survey is not completed yet and it is an intermediate results
            * @see surveyPostId
            * @see clientId
            */
        sendResult(postId?: string, clientId?: string, isPartialCompleted?: boolean): void;
        /**
            * It calls the [dxsurvey.com](http://www.dxsurvey.com) service and on callback fires onGetResult event with all answers that your users made for a question.
            * @param resultId [dxsurvey.com](http://www.dxsurvey.com) service resultId
            * @param name The question name
            * @see onGetResult
            */
        getResult(resultId: string, name: string): void;
        /**
            * Loads the survey Json from the [dxsurvey.com](http://www.dxsurvey.com) service. If clientId is not null and user has already completed the survey, the survey will go into "completedbefore" state.
            * @param surveyId [dxsurvey.com](http://www.dxsurvey.com) service surveyId
            * @param clientId indentificator for a user, for example e-mail or unique customer id in your web application.
            * @see state
            */
        loadSurveyFromService(surveyId?: string, cliendId?: string): void;
        protected onLoadingSurveyFromService(): void;
        protected onLoadSurveyFromService(): void;
        endLoadingFromJson(): void;
        protected onBeforeCreating(): void;
        protected onCreating(): void;
        hasVisibleQuestionByValueName(valueName: string): boolean;
        /**
            * Returns a variable value. Variable, unlike values, are not stored in the survey results.
            * @param name A variable name
            * @see SetVariable
            */
        getVariable(name: string): any;
        /**
            * Sets a variable value. Variable, unlike values, are not stored in the survey results.
            * @param name A variable name
            * @param newValue
            * @see GetVariable
            */
        setVariable(name: string, newValue: any): void;
        protected getUnbindValue(value: any): any;
        /**
            * Returns a question value
            * @param name A question name
            * @see data
            * @see setValue
            */
        getValue(name: string): any;
        /**
            * Sets a question value. It runs all triggers and conditions (visibleIf properties). Goes to the next page if goNextPageAutomatic is true and all questions on the current page are answered correctly.
            * @param name A question name
            * @param newValue
            * @see data
            * @see getValue
            * @see PageModel.visibleIf
            * @see QuestionBase.visibleIf
            * @see goNextPageAutomatic
            */
        setValue(name: string, newValue: any): void;
        protected doOnPageAdded(page: PageModel): void;
        protected tryGoNextPageAutomatic(name: string): void;
        /**
            * Returns the comment value
            * @param name
            * @see setComment
            */
        getComment(name: string): string;
        /**
            * Set the comment value
            * @param name
            * @param newValue
            * @see getComment
            */
        setComment(name: string, newValue: string): void;
        /**
            * Remove the value from the survey result.
            * @param {string} name The name of the value. Typically it is a question name
            */
        clearValue(name: string): void;
        questionVisibilityChanged(question: IQuestion, newValue: boolean): void;
        pageVisibilityChanged(page: IPage, newValue: boolean): void;
        panelVisibilityChanged(panel: IPanel, newValue: boolean): void;
        questionAdded(question: IQuestion, index: number, parentPanel: any, rootPanel: any): void;
        questionRemoved(question: IQuestion): void;
        panelAdded(panel: IElement, index: number, parentPanel: any, rootPanel: any): void;
        panelRemoved(panel: IElement): void;
        validateQuestion(question: IQuestion): SurveyError;
        validatePanel(panel: IPanel): SurveyError;
        processHtml(html: string): string;
        processText(text: string, returnDisplayValue: boolean): string;
        processTextEx(text: string): any;
        /**
            * Returns the number of corrected answers on quiz
            */
        getCorrectedAnswerCount(): number;
        /**
            * Returns the number of incorrected answers on quiz
            */
        getInCorrectedAnswerCount(): number;
        getCorrectedAnswers(): number;
        getInCorrectedAnswers(): number;
        /**
            * Set it to 'top' or 'bottom' if you want to show the Panel with information about how much time the end-user spent of the survey/page.
            * If the value doesn't equal 'none' then survey calls startTimer() method on survey rendering.
            * @see showTimerPanelMode
            * @see startTimer
            * @see stopTimer
            */
        showTimerPanel: string;
        readonly isTimerPanelShowingOnTop: boolean;
        readonly isTimerPanelShowingOnBottom: boolean;
        /**
            * Set this property to 'page' or 'survey' to show the timer information for page or survey only.
            * Use onTimerPanelInfoText event to change the default text.
            * @see showTimerPanel
            * @see onTimerPanelInfoText
            */
        showTimerPanelMode: string;
        readonly timerInfoText: string;
        /**
            * Call this method to start timer that will calculate how much time end-user spends on the survey or on pages
            * @see stopTimer
            * @see timeSpent
            */
        startTimer(): void;
        startTimerFromUI(): void;
        /**
            * Stop the timer.
            * @see startTimer
            * @see timeSpent
            */
        stopTimer(): void;
        /**
            * Returns the time in seconds end-user spends on the survey
            * @see startTimer
            * @see PageModel.timeSpent
            */
        readonly timeSpent: any;
        /**
            * The maximum time in seconds that end-user has to complete the survey. If the value is 0 or less, the end-user has unlimited number of time to finish the survey.
            * @see startTimer
            * @see maxTimeToFinishPage
            */
        maxTimeToFinish: number;
        /**
            * The maximum time in seconds that end-user has to complete a page in the survey. If the value is 0 or less, the end-user has unlimited time. You may override this value for every page.
            * @see startTimer
            * @see maxTimeToFinish
            * @see PageModel.maxTimeToFinish
            */
        maxTimeToFinishPage: number;
        protected doTimer(): void;
        geSurveyData(): ISurveyData;
        getSurvey(): ISurvey;
        getTextProcessor(): ITextProcessor;
        getObjects(pages: string[], questions: string[]): any[];
        setTriggerValue(name: string, value: any, isVariable: boolean): void;
}

/**
    * A base class for all triggers.
    * A trigger calls a method when the expression change the result: from false to true or from true to false.
    * Please note, it runs only one changing the expression result.
    */
export declare class Trigger extends Base {
        static operatorsValue: HashTable<Function>;
        static readonly operators: HashTable<Function>;
        value: any;
        constructor();
        getType(): string;
        operator: string;
        check(value: any): void;
        protected onSuccess(): void;
        protected onFailure(): void;
}
export interface ISurveyTriggerOwner {
        getObjects(pages: string[], questions: string[]): any[];
        doComplete(): any;
        setTriggerValue(name: string, value: any, isVariable: boolean): any;
}
/**
    * It extends the Trigger base class and add properties required for SurveyJS classes.
    */
export declare class SurveyTrigger extends Trigger {
        name: string;
        protected owner: ISurveyTriggerOwner;
        constructor();
        setOwner(owner: ISurveyTriggerOwner): void;
        readonly isOnNextPage: boolean;
}
/**
    * If expression returns true, it makes questions/pages visible.
    * Ohterwise it makes them invisible.
    */
export declare class SurveyTriggerVisible extends SurveyTrigger {
        pages: string[];
        questions: string[];
        constructor();
        getType(): string;
        protected onSuccess(): void;
        protected onFailure(): void;
        protected onItemSuccess(item: any): void;
        protected onItemFailure(item: any): void;
}
/**
    * If expression returns true, it completes the survey.
    */
export declare class SurveyTriggerComplete extends SurveyTrigger {
        constructor();
        getType(): string;
        readonly isOnNextPage: boolean;
        protected onSuccess(): void;
}
export declare class SurveyTriggerSetValue extends SurveyTrigger {
        setToName: string;
        setValue: any;
        isVariable: boolean;
        constructor();
        getType(): string;
        protected onSuccess(): void;
}

/**
    * A Model for a survey running in the Window.
    */
export declare class SurveyWindowModel extends Base {
        static surveyElementName: string;
        surveyValue: SurveyModel;
        windowElement: HTMLDivElement;
        templateValue: string;
        expandedChangedCallback: () => void;
        showingChangedCallback: () => void;
        constructor(jsonObj: any, initialModel?: SurveyModel);
        getType(): string;
        /**
            * A survey object.
            * @see SurveyModel
            */
        readonly survey: SurveyModel;
        /**
            * Returns true if the window is currently showing. Set it to true to show the window and false to hide it.
            * @see show
            * @see hide
            */
        isShowing: boolean;
        /**
            * Show the window
            * @see hide
            * @see isShowing
            */
        show(): void;
        /**
            * Hide the window
            * @see show
            * @see isShowing
            */
        hide(): void;
        /**
            * Returns true if the window is expanded. Set it to true to expand the window or false to collapse it.
            * @see expand
            * @see collapse
            */
        isExpanded: boolean;
        /**
            * The window and survey title.
            */
        title: string;
        readonly locTitle: LocalizableString;
        /**
            * Expand the window to show the survey.
            */
        expand(): void;
        /**
            * Collapse the window and show survey title only.
            */
        collapse(): void;
        protected createSurvey(jsonObj: any): SurveyModel;
        protected expandcollapse(value: boolean): void;
}

export declare class TextPreProcessorItem {
    start: number;
    end: number;
}
export declare class TextPreProcessorValue {
    name: string;
    returnDisplayValue: boolean;
    constructor(name: string, returnDisplayValue: boolean);
    value: any;
    isExists: boolean;
}
export declare class TextPreProcessor {
    onProcess: (textValue: TextPreProcessorValue) => void;
    constructor();
    process(text: string, returnDisplayValue?: boolean): string;
    readonly hasAllValuesOnLastRun: boolean;
}

/**
  * The class contains methods to work with www.dxsurvey.com service.
  */
export declare class dxSurveyService {
    static serviceUrl: string;
    constructor();
    loadSurvey(surveyId: string, onLoad: (success: boolean, result: string, response: any) => void): void;
    getSurveyJsonAndIsCompleted(surveyId: string, clientId: string, onLoad: (success: boolean, surveyJson: any, result: string, response: any) => void): void;
    sendResult(postId: string, result: JSON, onSendResult: (success: boolean, response: any) => void, clientId?: string, isPartialCompleted?: boolean): void;
    sendFile(postId: string, file: File, onSendFile: (success: boolean, response: any) => void): void;
    getResult(resultId: string, name: string, onGetResult: (success: boolean, data: any, dataList: Array<any>, response: any) => void): void;
    isCompleted(resultId: string, clientId: string, onIsCompleted: (success: boolean, result: string, response: any) => void): void;
}

export declare var surveyLocalization: {
    currentLocale: string;
    defaultLocale: string;
    locales: {};
    supportedLocales: any[];
    getString: (strName: string) => any;
    getLocales: () => string[];
};
export declare var surveyStrings: {
    pagePrevText: string;
    pageNextText: string;
    completeText: string;
    startSurveyText: string;
    otherItemText: string;
    progressText: string;
    emptySurvey: string;
    completingSurvey: string;
    completingSurveyBefore: string;
    loadingSurvey: string;
    optionsCaption: string;
    value: string;
    requiredError: string;
    requiredErrorInPanel: string;
    requiredInAllRowsError: string;
    numericError: string;
    textMinLength: string;
    textMaxLength: string;
    textMinMaxLength: string;
    minRowCountError: string;
    minSelectError: string;
    maxSelectError: string;
    numericMinMax: string;
    numericMin: string;
    numericMax: string;
    invalidEmail: string;
    urlRequestError: string;
    urlGetChoicesError: string;
    exceedMaxSize: string;
    otherRequiredError: string;
    uploadingFile: string;
    confirmDelete: string;
    keyDuplicationError: string;
    addRow: string;
    removeRow: string;
    addPanel: string;
    removePanel: string;
    choices_Item: string;
    matrix_column: string;
    matrix_row: string;
    savingData: string;
    savingDataError: string;
    savingDataSuccess: string;
    saveAgainButton: string;
    timerMin: string;
    timerSec: string;
    timerSpentAll: string;
    timerSpentPage: string;
    timerSpentSurvey: string;
    timerLimitAll: string;
    timerLimitPage: string;
    timerLimitSurvey: string;
};

export declare class QuestionCustomWidget {
        name: string;
        widgetJson: any;
        htmlTemplate: string;
        constructor(name: string, widgetJson: any);
        afterRender(question: IQuestion, el: any): void;
        willUnmount(question: IQuestion, el: any): void;
        isFit(question: IQuestion): boolean;
        activatedByChanged(activatedBy: string): void;
        readonly isDefaultRender: boolean;
}
export declare class CustomWidgetCollection {
        static Instance: CustomWidgetCollection;
        onCustomWidgetAdded: Event<(customWidget: QuestionCustomWidget) => any, any>;
        readonly widgets: Array<QuestionCustomWidget>;
        addCustomWidget(widgetJson: any, activatedBy?: string): void;
        /**
            * Returns the way the custom wiget is activated. It can be activated by a property ("property"), question type ("type") or by new/custom question type ("customtype").
            * @param widgetName the custom widget name
            * @see setActivatedBy
            */
        getActivatedBy(widgetName: string): any;
        /**
            * Sets the way the custom wiget is activated. The activation types are: property ("property"), question type ("type") or new/custom question type ("customtype"). A custom wiget may support all or only some of this activation types.
            * @param widgetName
            * @param activatedBy there are three possible variants: "property", "type" and "customtype"
            */
        setActivatedBy(widgetName: string, activatedBy: string): void;
        clear(): void;
        getCustomWidgetByName(name: string): QuestionCustomWidget;
        getCustomWidget(question: IQuestion): QuestionCustomWidget;
}

export declare class StylesManager {
    static Styles: {
        [key: string]: string;
    };
    static ThemeColors: {
        [key: string]: {
            [key: string]: string;
        };
    };
    static ThemeCss: {
        [key: string]: string;
    };
    static findSheet(styleSheetId: string): CSSStyleSheet;
    static createSheet(styleSheetId: string): CSSStyleSheet;
    static applyTheme(themeName?: string, themeSelector?: string): void;
    constructor();
    initializeStyles(sheet: CSSStyleSheet): void;
}

