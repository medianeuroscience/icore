/*
 * Contains code for Javascript Survey
 */

Survey.StylesManager.applyTheme("winter");

var likertScale = [
    {value: 1, text: "Strongly Disagree"},
    {value: 2, text: "Disagree"}, 
    {value: 3, text: "Somewhat Disagree"}, 
    {value: 4, text: "Neutral"}, 
    {value: 5, text: "Somewhat Agree"}, 
    {value: 6, text: "Agree"}, 
    {value: 7, text: "Strongly Agree"}
];

var persuasionScale = [
    {value: 1, text: "Not Persuasive at all"},
    {value: 2, text: "Not Persuasive"}, 
    {value: 3, text: "Somewhat not Persuasive "}, 
    {value: 4, text: "Neutral"}, 
    {value: 5, text: "Somewhat Persuasive"}, 
    {value: 6, text: "Persuasive"}, 
    {value: 7, text: "Very Persuasive"}
];

var pre_matrix = { 
	questions: [
    {
        type: "matrix",
        name: "accuracy",
        isAllRowRequired:hybrid_required,
        title: "Please indicate whether you agree or disagree with the following statements about the recommendation's accuracy.",
        columns: likertScale,
        rows: [
            {value: "item1",text: "The recommended artist represents my tastes."},
            {value: "item2",text: "I dislike the recommended artist."}, 
            {value: "item3",text: "Considering my tastes, this is a bad recommendation."}, 
            {value: "item4",text: "This is an accurate recommendation."}, 
            {value: "item5",text: "I like the recommended artist."}, 
        ]
    },
    {
        type: "matrix",
        name: "novelty",
        isAllRowRequired:hybrid_required,
        title: "Please indicate whether you agree or disagree with the following statements about the recommendation's novelty.",
        columns: likertScale,
        rows: [
            {value: "item1",text: "I have never listened to this artist before."},
            {value: "item2",text: "I am aware of the recommended artist.."}, 
            {value: "item3",text: "I am surprised this artist was recommended."}, 
            {value: "item4",text: "I would not have found this artist on my own."}, 
            {value: "item5",text: "The recommended artist is new to me."}, 
        ]
    }
    
]};

var post_matrix = { 
		questions: [
	    {
	        type: "matrix",
	        name: "accuracy",
	        isAllRowRequired:hybrid_required,
	        title: "Please indicate whether you agree or disagree with the following statements about the recommendation's accuracy.",
	        columns: likertScale,
	        rows: [
	            {value: "item1",text: "The recommended artist represents my tastes."},
	            {value: "item2",text: "I dislike the recommended artist."}, 
	            {value: "item3",text: "Considering my tastes, this is a bad recommendation."}, 
	            {value: "item4",text: "This is an accurate recommendation."}, 
	            {value: "item5",text: "I like the recommended artist."}, 
	        ]
	    },
	    {
	        type: "matrix",
	        name: "novelty",
	        isAllRowRequired:hybrid_required,
	        title: "Please indicate whether you agree or disagree with the following statements about the recommendation's novelty.",
	        columns: likertScale,
	        rows: [
	            {value: "item1",text: "I have never listened to this artist before."},
	            {value: "item2",text: "I am aware of the recommended artist.."}, 
	            {value: "item3",text: "I am surprised this artist was recommended."}, 
	            {value: "item4",text: "I would not have found this artist on my own."}, 
	            {value: "item5",text: "The recommended artist is new to me."}, 
	        ]
	    }
	    
	]};
