(function($) {
	var globalDefault = {
		'gZoomStatus' : 0,
		'gScaleVal' : 0.7 
	};
	
	$.fn.generateMap = function(options) {
		var settings = $.extend({
            // These are the defaults.
            loadRegion: "bangladesh",
            regionData: {
				"bangladesh": [
					{
						"region_name": "Rangpur",
						"region_id": "rangpurDivision",
						"area": 800,
						"population": 9794525
					},
					{
						"region_name": "Dhaka",
						"region_id": "dhakaDivision",
						"area": 700,
						"population": 5769750
					},
					{
						"region_name": "Rajshahi",
						"region_id": "rajshahiDivision",
						"area": 600,
						"population": 5557276
					},
					{
						"region_name": "Sylhet",
						"region_id": "sylhetDivision",
						"area": 500,
						"population": 4999932
					}
				],
				"rangpurDivision": [
					{
						"parent_region_id" : "bangladesh",
						"region_name": "Panchagarh",
						"region_id": "panchagarhDistrict",
						"area": 8001,
						"population": 9794525
					},
					{
						"region_name": "Thakurgaon",
						"region_id": "thakurgaonDistrict",
						"area": 7001,
						"population": 5769750
					},
					{
						"region_name": "Dinajpur",
						"region_id": "dinajpurDistrict",
						"area": 6001,
						"population": 5557276
					},
					{
						"region_name": "Gaibandha",
						"region_id": "gaibandhaDistrict",
						"area": 5001,
						"population": 4999932
					},
					{
						"region_name": "Kurigram",
						"region_id": "kurigramDistrict",
						"area": 4001,
						"population": 4881756
					},
					{
						"region_name": "Rangpur",
						"region_id": "rangpurDistrict",
						"area": 3001,
						"population": 4377487
					},
					{
						"region_name": "Nilfamari",
						"region_id": "nilphamariDistrict",
						"area": 2001,
						"population": 4374052
					},
					{
						"region_name": "Lalmonirhat",
						"region_id": "lalmonirhatDistrict",
						"area": 1001,
						"population": 4050803
					}
				],
				"rajshahiDivision": [
					{
						"parent_region_id" : "bangladesh",
						"region_name": "Nawabganj",
						"region_id": "nawabganjDistrict",
						"area": 801,
						"population": 9794525
					},
					{
						"region_name": "Naogaon",
						"region_id": "naogaonDistrict",
						"area": 701,
						"population": 5769750
					},
					{
						"region_name": "Joypurhat",
						"region_id": "joypurhatDistrict",
						"area": 601,
						"population": 5557276
					},
					{
						"region_name": "Bogra",
						"region_id": "bograDistrict",
						"area": 501,
						"population": 4999932
					},
					{
						"region_name": "Sirajganj",
						"region_id": "sirajganjDistrict",
						"area": 401,
						"population": 4881756
					},
					{
						"region_name": "Pabna",
						"region_id": "pabnaDistrict",
						"area": 301,
						"population": 4377487
					},
					{
						"region_name": "Natore",
						"region_id": "natoreDistrict",
						"area": 201,
						"population": 4374052
					},
					{
						"region_name": "Rajshahi",
						"region_id": "rajshahiDistrict",
						"area": 101,
						"population": 4050803
					}
				],
				"rangpurDistrict": [
					{
						"parent_region_id" : "rangpurDivision",
						"region_name": "Gangachhara",
						"region_id": "gangachharaUpazila",
						"area": 5001,
						"population": 9794525
					},
					{
						"region_name": "Badarganj",
						"region_id": "badarganjUpazila",
						"area": 5001,
						"population": 5769750
					},
					{
						"region_name": "Taraganj",
						"region_id": "taraganjUpazila",
						"area": 5001,
						"population": 5557276
					},
					{
						"region_name": "Pirganj",
						"region_id": "pirganjUpazila",
						"area": 5001,
						"population": 4999932
					},
					{
						"region_name": "Mithapukur",
						"region_id": "mithapukurUpazila",
						"area": 5001,
						"population": 4881756
					},
					{
						"region_name": "Rangpur Sadar",
						"region_id": "rangpursadarUpazila",
						"area": 5001,
						"population": 4377487
					},
					{
						"region_name": "Kaunia",
						"region_id": "kauniaUpazila",
						"area": 5001,
						"population": 4374052
					},
					{
						"region_name": "Pirgachha",
						"region_id": "pirgachhaUpazila",
						"area": 5001,
						"population": 4050803
					}
				]
			},
			'parentForDistrictMap' : {
				"bargunaDistrict":"barisalDivision","barisalDistrict":"barisalDivision","bholaDistrict":"barisalDivision","jhalakathiDistrict":"barisalDivision","patuakhaliDistrict":"barisalDivision","pirojpurDistrict":"barisalDivision","bandarbanDistrict":"chittagongDivision","brahmanbariaDistrict":"chittagongDivision","chandpurDistrict":"chittagongDivision","chittagongDistrict":"chittagongDivision","comillaDistrict":"chittagongDivision","coxsbazarDistrict":"chittagongDivision","feniDistrict":"chittagongDivision","khagrachhariDistrict":"chittagongDivision","lakshmipurDistrict":"chittagongDivision","noakhaliDistrict":"chittagongDivision","rangamatiDistrict":"chittagongDivision","dhakaDistrict":"dhakaDivision","faridpurDistrict":"dhakaDivision","gazipurDistrict":"dhakaDivision","gopalganjDistrict":"dhakaDivision","jamalpurDistrict":"mymensinghDivision","kishoreganjDistrict":"dhakaDivision","madaripurDistrict":"dhakaDivision","manikganjDistrict":"dhakaDivision","munshiganjDistrict":"dhakaDivision","mymensinghDistrict":"mymensinghDivision","narayanganjDistrict":"dhakaDivision","narsingdiDistrict":"dhakaDivision","netrokonaDistrict":"mymensinghDivision","rajbariDistrict":"dhakaDivision","shariatpurDistrict":"dhakaDivision","sherpurDistrict":"mymensinghDivision","tangailDistrict":"dhakaDivision","bagerhatDistrict":"khulnaDivision","chuadangaDistrict":"khulnaDivision","jessoreDistrict":"khulnaDivision","jhenaidahDistrict":"khulnaDivision","khulnaDistrict":"khulnaDivision","kushtiaDistrict":"khulnaDivision","maguraDistrict":"khulnaDivision","meherpurDistrict":"khulnaDivision","narailDistrict":"khulnaDivision","satkhiraDistrict":"khulnaDivision","bograDistrict":"rajshahiDivision","joypurhatDistrict":"rajshahiDivision","naogaonDistrict":"rajshahiDivision","natoreDistrict":"rajshahiDivision","chapainawabganjDistrict":"rajshahiDivision","pabnaDistrict":"rajshahiDivision","rajshahiDistrict":"rajshahiDivision","sirajganjDistrict":"rajshahiDivision","dinajpurDistrict":"rangpurDivision","gaibandhaDistrict":"rangpurDivision","kurigramDistrict":"rangpurDivision","lalmonirhatDistrict":"rangpurDivision","nilphamariDistrict":"rangpurDivision","panchagarhDistrict":"rangpurDivision","rangpurDistrict":"rangpurDivision","thakurgaonDistrict":"rangpurDivision","habiganjDistrict":"sylhetDivision","maulvibazarDistrict":"sylhetDivision","sunamganjDistrict":"sylhetDivision","sylhetDistrict":"sylhetDivision"
			},
			'infosForconstituency' : {},
			_this : this,
            backgroundColor: "white"
        }, options );
		
		setTimeout(function() { changeSVG(settings.loadRegion, settings.regionData, settings.infosForconstituency, settings.parentForDistrictMap, settings._this); }, 1000);
		
		// EVENT : START ------------------------------ 

		$(document).on('click', '.map g path:not(.notbtn), .map g g:not(.notbtn)', function(e) {
			e.preventDefault();
			var mapAttr = $(this).attr('id');
			//console.log('mapAttr : ' + mapAttr);
			//console.log('settings.regionData : ' + settings.regionData);
			changeSVG(mapAttr, settings.regionData, settings.infosForconstituency, settings.parentForDistrictMap, settings._this);
		});

		$(document).on('click', '.backbtn', function(e) {
			e.preventDefault();

			var mapParentAttr = $(this).attr('parent-id') ? $(this).attr('parent-id') : 'bangladesh';

			if(mapParentAttr != undefined && bangladeshSVG[mapParentAttr] != undefined) {
				changeSVG(mapParentAttr, settings.regionData, settings.infosForconstituency, settings.parentForDistrictMap, settings._this);
			} 
			
		});
		
		$(document).on('click', '.zoom_map', function(e) {
			e.preventDefault();
			var zoomId = $(this).attr('id');

			//console.log('zoomId : ' + zoomId);
			
			zoomToggle(zoomId);
		});
		
		// EVENT : END ------------------------------ 
		
	}
	
	function changeSVG(x, regionData, infosForconstituency, parentForDistrictMap, _this)
	{
		$('.info_panel').remove();
		//console.log('_this : ' + _this);
 
		if(bangladeshSVG[x] != undefined) {
			
			var svgMapHtml = '';
			var backbtn = '';
			var styleTagContent = '';
			var remainPreTag = '</style>';

			var svgTagDef = '<svg version="1.1" id="Bangladesh" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" x="0px" y="0px" width="575.6px" height="600px" viewBox="0 0 445.6 510.6" enable-background="new 0 0 445.6 510.6" xml:space="preserve">';
			

			//var firstTitle = 'Test Test  Test Test Test Test Test Test Test Test Test Test Test';
			//var sourceName = $('#source_id option:selected').text() ? $('#source_id option:selected').text() : '';
			//var cMapTitle = sourceName + capitalizeFirstLetter(x);
			//var mapTitle = '<text id="map_title" y="15"> '+ firstTitle +'</text> <text id="c_map_title" x="80" y="30"> '+ cMapTitle +'</text>';
			
			var cMapTitle = capitalizeFirstLetter(x);
			var mapTitle = '<text id="map_title" x="80" y="30"> '+ cMapTitle +'</text>';
				
			var zoombtn = '<g id="zoom_in" class="zoom_map notbtn"><path class="notbtn" fill="#F7F7F7" stroke="#CCCCCC" d="M8.368,67.282h23v0.25h0.049l0.05-19.449c0.101-2.5-2.699-2.4-2.699-2.4l-17-0.1c-4.101-0.4-3.7,2.898-3.7,2.898v19.051h0.3V67.282z" fill-opacity="0.5" stroke-opacity="0.8" /><rect x="8.368" y="67.282" fill="#F7F7F7" width="23" height="0.25" /><text id="zoom_in" transform="matrix(1 0 0 1 15.4065 60.5659)" fill="#4C4C4D" font-family="Tahoma" font-size="14">+</text></g> <g id="zoom_out" class="zoom_map notbtn"><path class="notbtn" fill="#F7F7F7" stroke="#CCCCCC" d="M31.342,67.919h-23l0,0l-0.271-0.032L8.042,86.121c0.2,3,2.5,3.1,2.5,3.1h17.1c3.7-0.199,3.7-2.801,3.7-2.801l0.042-18.49l0,0L31.342,67.919z" fill-opacity="0.5" stroke-opacity="0.8" /><text id="zoom_out" transform="matrix(1 0 0 1 17.7898 83.0659)" fill="#4C4C4D" font-family="Tahoma" font-size="14">-</text></g>';
			

			var svgMapHtmlArr = bangladeshSVG[x].split("</style>");	
			
			if(svgMapHtmlArr[1] == undefined) {
				remainPreTag = 'xml:space="preserve">';
				svgMapHtmlArr = bangladeshSVG[x].split('xml:space="preserve">');
			} else {
				styleTagContent = (svgMapHtmlArr[0].split('<style type="text/css">')[1] !=undefined) ? svgMapHtmlArr[0].split('<style type="text/css">')[1] : '';
				
				if(styleTagContent == '') {
					styleTagContent = (svgMapHtmlArr[0].split('<style>')[1] !=undefined) ? svgMapHtmlArr[0].split('<style>')[1] : '';
				}
			}
				

			if(x != 'bangladesh') {
 
				var parent_region_id = 'bangladesh';
				
				if(regionData[x] != undefined) {
					var parent_region_id = regionData[x][0].parent_region_id ? regionData[x][0].parent_region_id : 'bangladesh';
				}
			
				if(parent_region_id == undefined) {
					alert('No Data');
					return;
				}

				var backbtn = '<g id="backbtn" class="backbtn notbtn" parent-id="'+ parent_region_id +'"> <rect x="414.1" y="46.403" fill="#F7F7F7" stroke="#CCCCCC" stroke-width="0.5" stroke-miterlimit="10" width="48.666" height="17" fill-opacity="0.5" stroke-opacity="0.8" /><text transform="matrix(1 0 0 1 425.8352 58.7896)" fill="#444444" font-family="Tahoma" font-size="12">Back</text> </g>';
				
			} 
		
			
			if(x!= undefined && x!='' && x!='bangladesh' && x !='allDistrict' ) {
				gScaleVal = (globalDefault.gZoomStatus == 1) ? gScaleVal : 0.7;
				var groupG = '<g id="zoomInOut" class="zoomInOut drag" transform="translate(40,50) scale('+gScaleVal+')" animator="1">';
			} else if(x!= undefined && x!='' && x =='allDistrict'){
				gScaleVal = (globalDefault.gZoomStatus == 1) ? gScaleVal : 0.7;
				var groupG = '<g id="zoomInOut" class="zoomInOut drag" transform="translate(0,40) scale('+gScaleVal+')" animator="1">';
			} else {
				gScaleVal = (globalDefault.gZoomStatus == 1) ? gScaleVal : 0.9;
				var groupG = '<g id="zoomInOut" class="zoomInOut drag" transform="translate(40,0) scale('+gScaleVal+')" animator="1">';
			}	 
			
			var lastSvgArr = svgMapHtmlArr[1];
				lastSvgArr = lastSvgArr.split("</svg>");
			var lastSVG = lastSvgArr[0] + '</g>';
			var lastSvgTag = '</svg>';

			if(styleTagContent !='') {
				styleTagContent = '<style>' + styleTagContent + '</style>';
			}
			
			var useSVGTag = '';
			var titleAndZoomBtn = '<g id="show_up_in_map" class="notbtn">' + mapTitle + zoombtn + backbtn + '</g>';

			svgMapHtml = svgTagDef + styleTagContent + remainPreTag + groupG + lastSVG + titleAndZoomBtn + useSVGTag + lastSvgTag;
			//$("#changeSvgMap").html(svgMapHtml);
			$(_this).html(svgMapHtml);
			
			if(x!= undefined && x!='' && x!='bangladesh' && x !='allDistrict') {
				//document.getElementById('changeSvgMap').style.height = '610px';
				$(_this).height('610px');
			} else if(x!= undefined && x!='' && x =='allDistrict'){
				$(_this).height('730px');
			} else {
				$(_this).height('610px');
			} 
			
			var jsonConstituencyUpazilas = infosForconstituency.constituencyUpazilasData;
			
			if(x.includes("District") && typeof jsonConstituencyUpazilas != 'undefined') {
				var districtWiseUpazilas = infosForconstituency.districtWiseUpazilas;

				//console.log('jsonConstituencyUpazilas : ' + JSON.stringify(jsonConstituencyUpazilas));
				//console.log('districtWiseUpazilas : ' + JSON.stringify(districtWiseUpazilas));

				for(e in districtWiseUpazilas) {
					$('#'+ districtWiseUpazilas[e])
					.css({'fill-opacity': '0.1'});
					//console.log('districtWiseUpazilas[e] : ' + districtWiseUpazilas[e]);
				}
				
				for(c in jsonConstituencyUpazilas) {
					$('#'+ jsonConstituencyUpazilas[c])
					.css({'fill': 'rgba(64, 0, 128, 0.7)', 'fill-opacity': '0.7'});
				}
			}
			
			if(x != undefined && regionData[x] != undefined) {
				changeSVGMapVal(regionData[x]);
			} else {
				
				if(x.search("District") > -1) {
					$("g#backbtn").attr('parent-id', parentForDistrictMap[x]);
				}
			}
			
			
			gDragDropStatus = 0;
			dragDrop();
		}
	}
	
	function changeSVGMapVal(regionData)
	{
		
		//console.log('regionData : ' + JSON.stringify(regionData));
		for(i = 0; i < regionData.length; i++) {
 
			var cssOptions = {'fill-opacity' : '1'};

			if(typeof regionData[i].hasConstituency != 'undefined' && regionData[i].hasConstituency == 1) {
				//cssOptions.fill = 'rgba(128, 94, 74, 1)';
				cssOptions.fill = 'rgba(64, 0, 128, 1)';
			}
 
			$('#'+ regionData[i].region_id) 
			//.css({'fill': 'rgba(11, 104, 170,' + regionData[i].population/highest_value +')'})
			//.css({'fill-opacity': '1'})
			.css(cssOptions)
			.data('bangladesh_region', regionData[i]);
		}

		$('.map g#zoomInOut path:not(.notbtn), .map g#zoomInOut g:not(.notbtn)').mouseover(function (e) {
			var regionData = $(this).data('bangladesh_region') ? $(this).data('bangladesh_region') : '';
			var region_name = regionData.region_name ? regionData.region_name : '';
			var nationalArea = regionData.area ? 'Area: '+ regionData.area.toLocaleString("en-UK") + '<br>' : '';
			var nationalPopulation = regionData.population ? 'Population: '+ regionData.population.toLocaleString("en-UK") + '<br>' : '';
			// var disaggreegation = regionData.disaggreegation ? regionData.disaggreegation: '';
 
			// var disaggreegationData = '';
			
			// if(disaggreegation != undefined) {
				// for (var d in disaggreegation){
					// if (disaggreegation.hasOwnProperty(d)) {
						// disaggreegationData += ' '+ d + ' : '+ disaggreegation[d] + '<br>';
					// }
				// }
			// }
			
			var hoverData = '';
			if(regionData.region_name != undefined) {
				//hoverData = regionData.region_name + '<br>' + nationalArea + '<br>' + disaggreegation;
				hoverData = regionData.region_name + '<br>' + nationalArea + nationalPopulation;;
			} else {
				hoverData = 'No Data';
			}

			$('<div class="info_panel">'+ hoverData +'</div>').appendTo('body');
		})
		.mouseleave(function () {
			$('.info_panel').remove();
		})
		.mousemove(function(e) {
			var mouseX = e.pageX, //X coordinates of mouse
				mouseY = e.pageY; //Y coordinates of mouse

			$('.info_panel').css({
				top: mouseY-100,
				left: mouseX - ($('.info_panel').width()/2)
			});
		});
	}

	function zoomToggle(zoomId) 
	{
 
		var zoomInOut = document.getElementById('zoomInOut');
		
		if(zoomId == 'zoom_in') {
			//gZoomStatus = 1;
			gScaleVal = 1.3;
			zoomInOut.setAttribute('transform', 'translate(-80, -10) scale('+gScaleVal+')');
		} else if(zoomId == 'zoom_out') {
			
			//gZoomStatus = 0;
			gScaleVal = 0.7;
			zoomInOut.setAttribute('transform', 'translate(40, 50) scale('+gScaleVal+')');
		} 
	}

	function capitalizeFirstLetter(string) {
		string = string.replace(/([A-Z])/g, ' $1').trim();
		//return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase();
		return toTitleCase(string);
	}

	function toTitleCase(str)
	{
		return str.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
	}
	
	function dragDrop()
	{
		var svg   = document.getElementsByTagName('svg')[0];
		var svgNS = svg.getAttribute('xmlns');
		var pt    = svg.createSVGPoint(); 
		  
		function cursorPoint(evt){
			pt.x = evt.clientX; pt.y = evt.clientY;
			return pt.matrixTransform(svg.getScreenCTM().inverse());
		}

		for (var a=svg.querySelectorAll('.drag'),i=0,len=a.length;i<len;++i){
			(function(el){
				
				var onmove; // make inner closure available for unregistration
				el.addEventListener('mousedown',function(e){
				//el.parentNode.appendChild(el); // move to top
				var x = el.tagName=='circle' ? 'cx' : 'x';
				var y = el.tagName=='circle' ? 'cy' : 'y';
				var mouseStart   = cursorPoint(e);
	
				onmove = function(e){

					var current = cursorPoint(e);
				  
					pt.x = current.x - mouseStart.x;
					pt.y = current.y - mouseStart.y;

				  
					var rX = pt.x;
					var rY = pt.y;
				 
					el.setAttribute('transform', 'translate(' + (rX) + ', ' + (rY) + ') scale('+gScaleVal+')');
				  
					gDragDropStatus = 1;

				};
				document.body.addEventListener('mousemove', onmove, false);
	
			  },false);
			  document.body.addEventListener('mouseup',function(){
				document.body.removeEventListener('mousemove',onmove,false);
			  },false);
			})(a[i]);
		}
		
	}
	
	
})(jQuery);