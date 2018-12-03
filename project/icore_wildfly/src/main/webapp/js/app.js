/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
Core JavaScript functionality for the application.  Performs the required
Restful calls, validates return values, and populates the member table.
 */

var member = "";
var memberList = [];

/* Builds the updated table for the member list */
function buildMemberRows(members) {
    return _.template( $( "#member-tmpl" ).html(), {"members": members});
}

function getMember( amtid ) {
	console.log( "Looking up: " + JSON.stringify(amtid) );
    $.ajax({
        url: "rest/members/" + amtid,
        cache: false,
        dataType: "json",
        success: function(data) {
        	member = data;

        	//showCluster();
    		
        	console.log("Finished loading JSON");
        	if ( member.preData == "none" ) {
        		startSurvey();
        	}
        	else {
        		showThankYouMessage();
        	}

        },
        error: function(error) {
        	$('#loader').remove();
            console.log("error getting member -" + error.status);
            showNoMemberMessage();
        }
    });
}

function showNoMemberMessage() {
	console.log("Showing error message");
	$('#imagerec').hide();
	$('#psl-survey-container').show().css('display','flex');
	$('#textexpl').html( "Sorry, we could not find your AMTID. <br>" +
			"If you believe you are seeing this message in error, please e-mail <span style='color:#D87300; font-weight:bold'>j.au.schaffer@gmail.com</span>." );
	$('#textexpl').show();
}

function showErrorMessage() {
	$('#imagerec').hide();
	$('#psl-survey-container').show().css('display','flex');
	$('#textexpl').html( "Sorry, we could not build your recommendations.  This is because you have not liked enough artists in your last.fm account, or you are not following enough users.<br>" +
			"If you would like to continue with the study, <span style='color:#D87300; font-weight:bold'>please follow the directions located <a target=\"blank\" href=\"https://docs.google.com/document/d/1ROI1BrYKkfLw_O9uLWROESKigCYa4wiKUeQOxxkEfnk/edit\">here</a></span><br>" +
			"<span style='color:#D87300; font-weight:bold'>Then, click the below button</span>.  If you believe you are seeing this message in error, please e-mail <span style='color:#D87300; font-weight:bold'>j.au.schaffer@gmail.com</span>." );
    $('#textexpl').show();
    document.getElementById("crawlagainbutton").onclick = tryCrawlAgain;
    $('#crawlagainbutton').show();
}

function shuffle(array) {
  var currentIndex = array.length, temporaryValue, randomIndex;

  // While there remain elements to shuffle...
  while (0 !== currentIndex) {

    // Pick a remaining element...
    randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex -= 1;

    // And swap it with the current element.
    temporaryValue = array[currentIndex];
    array[currentIndex] = array[randomIndex];
    array[randomIndex] = temporaryValue;
  }

  return array;
}

//<a href="https://www.w3schools.com">Visit W3Schools</a>

function showThankYouMessage() {
	$('#imagerec').hide();
	$('#psl-survey-container').show().css('display','flex');
//	$('#textexpl').html( "Thank you for completing our study!  Your completion code is <span style='color:#D87300; font-weight:bold'>" + member.completionCode + "</span>" );
//    $('#textexpl').show();
	thankYou();
}

function questionnaire( pageNumber ) {
    $('#textexpl').html( "If you have never heard this artist, <span style='color:#D87300; font-weight:bold'>click the image</span> to listen to a preview of their music (a new tab will be opened).  " +
    		"Then, <span style='color:#D87300; font-weight:bold'>please answer the questions below</span>.  If you have already heard this artist, please proceed directly to the questions below." );
}

function startSurvey() {
	
	questionnaire();

    //	------------------------------------------------------------------
	$('#psl-survey-container').show().css('display','flex');
	window.survey = new Survey.Model(pre_matrix);
    $("#surveyElement").Survey({model:survey});
    survey.showPrevButton = false;
    
    survey.onCurrentPageChanged.add(function (result) {
    	$('html, body').animate({ scrollTop: 0 }, 'medium');
    	$('#svgexpl').hide();
    	$('#textrec').html( $('#textrec').html().replace("recommend ","recommended " ) );
    	
    	var data_index = survey.currentPage.questions[ 0 ].name.split("::")[1];
    	if ( survey.currentPage.questions[0].name.includes("rando") ) {
    		rando( data_index );
    	}
    	if ( survey.currentPage.questions[0].name.includes("accuracy") ) {
    		accnov( data_index );
    	}
    	if ( survey.currentPage.questions[0].name.includes("style") ) {
    		expl( data_index );
    	}
    	if ( survey.currentPage.questions[0].name.includes("rank") ) {
    		rank( data_index );
    	}
    	if ( survey.currentPage.questions[0].name.includes("text") ) {
    		simpleExpl( data_index );
    	}
    	if ( survey.currentPage.questions[0].name.includes("collapsible") ) {
    		collapsible( data_index );
    	}
    	if ( survey.currentPage.questions[0].name.includes("venn") ) {
    		john_venn(data_index);
    	}
    	if ( survey.currentPage.questions[0].name.includes("cluster") ) {
    		cluster(data_index);
    	}
    	if ( survey.currentPage.questions[0].name.includes("suggestions") ) {
    		suggestions(data_index);
    	}

    });

    survey.onComplete.add(function (result) {
    	console.log("Survey finished");
    	$('#textrec').hide();
    	$('#bandlink').hide();
    	$('#textexpl').hide();
    	
    	member.rdata = JSON.stringify( survey.data );
    	saveData( member );
    	
    });

}

function thankYou() {
    $('#textexpl').html( "Thank you for completing our study!" );
    $('#textexpl').show();
}

function suggestions( pageNumber ) {
	$('#textrec').hide();
	$('#bandlink').hide();
	$('#textexpl').hide();
}

/* Uses JAX-RS GET to retrieve current member list */
function updateMemberTable() {
    $.ajax({
        url: "rest/members",
        cache: false,
        success: function(data) {
            //$('#members').empty().append(buildMemberRows(data));
            
//            $$("webixlist").clearAll();
//            for (i=0; i < data.length; i++) {
//            	$$("webixlist").add(data[i]);
//            }
        	
        	for ( i = 0; i < data.length; i++ ) {
        		memberList[ i ] = data[ i ];
        		console.log( "members: " + memberList[ i ].amtid );
        	}
        	
        },
        error: function(error) {
            console.log("error updating table -" + error.status);
        }
    });
}

/*
Attempts to register a new member using a JAX-RS POST.  The callbacks
the refresh the member table, or process JAX-RS response codes to update
the validation errors.
 */
function registerMember(memberData) {
    //clear existing  msgs
    $('span.invalid').remove();
    $('span.success').remove();
    console.log( "Aight sendin' dat yo yo b-boy");

    $.ajax({
        url: 'rest/members',
        contentType: "application/json",
        dataType: "json",
        type: "POST",
        data: JSON.stringify(memberData),
        success: function(data) {
        	console.log("sent member data");
        },
        error: function(error) {
            if ((error.status == 409) || (error.status == 400)) {
                //console.log("Validation error registering user!");

                var errorMsg = $.parseJSON(error.responseText);
                console.log(errorMsg);

                $.each(errorMsg, function(index, val) {
                    $('<span class="invalid">' + val + '</span>').insertAfter($('#' + index));
                });
            } else {
                //console.log("error - unknown server issue");
                $('#formMsgs').append($('<span class="invalid">Unknown server error</span>'));
            }
        }
    });
}

var reset = false;
function saveData(memberData) {
    $.ajax({
        url: 'rest/saves',
        contentType: "application/json",
        dataType: "json",
        type: "POST",
        data: JSON.stringify(memberData),
        success: function(data) {
            console.log("member data was saved");
            if ( reset )
            	location.reload(true);
        },
        error: function(error) {
            if ((error.status == 409) || (error.status == 400)) {
                //console.log("Validation error registering user!");

                var errorMsg = $.parseJSON(error.responseText);
                console.log(errorMsg);

                $.each(errorMsg, function(index, val) {
                    $('<span class="invalid">' + val + '</span>').insertAfter($('#' + index));
                });
            } else {
                //console.log("error - unknown server issue");
                $('#formMsgs').append($('<span class="invalid">Unknown server error</span>'));
            }
        }
    });
}

function hellojboss() {

    $.ajax({
        url: 'rest/hello',
        contentType: "application/json",
        dataType: "json",
        type: "POST",
        success: function(data) {
            //console.log("Member registered");
        },
        error: function(error) {
            if ((error.status == 409) || (error.status == 400)) {
                //console.log("Validation error registering user!");

                var errorMsg = $.parseJSON(error.responseText);

                $.each(errorMsg, function(index, val) {
                    $('<span class="invalid">' + val + '</span>').insertAfter($('#' + index));
                });
            } else {
                //console.log("error - unknown server issue");
                $('#formMsgs').append($('<span class="invalid">Unknown server error</span>'));
            }
        }
    });
}

function getUrlParameter(sParam) {
    var sPageURL = decodeURIComponent(window.location.search.substring(1)),
        sURLVariables = sPageURL.split('&'),
        sParameterName,
        i;

    for (i = 0; i < sURLVariables.length; i++) {
        sParameterName = sURLVariables[i].split('=');

        if (sParameterName[0] === sParam) {
            return sParameterName[1] === undefined ? true : sParameterName[1];
        }
    }
};

