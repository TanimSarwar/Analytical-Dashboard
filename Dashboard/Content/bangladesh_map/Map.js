
$(document).ready(function () {
	var regionsOfBangladeshDemo = {
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
			},
			{
				"region_name": "Mymensingh",
				"region_id": "mymensinghDivision",
				"area": 400,
				"population": 4881756
			},
			{
				"region_name": "Barishal",
				"region_id": "barisalDivision",
				"area": 300,
				"population": 4377487
			}
			,
			{
				"region_name": "Khulna",
				"region_id": "khulnaDivision",
				"area": 200,
				"population": 4374052
			},
			{
				"region_name": "Chittagong",
				"region_id": "chittagongDivision",
				"area": 100,
				"population": 4050803
			}
		],
		"rangpurDivision": [
			{
				"parent_region_id": "bangladesh",
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
				"parent_region_id": "bangladesh",
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
		"barisalDivision": [
			{
				"region_name": "Barguna",
				"color_code": "#a1998;",
				"region_id": "bargunaDistrict",
				"area": 7001,
				"population": 13434534,
				"parent_region_name": "Bangladesh",
				"parent_region_id": "bangladesh"
			},
			{
				"region_name": "Barisal",
				"color_code": "#a1998;",
				"region_id": "barisalDistrict",
				"area": 7002,
				"population": 3434534,
			}
		],
		"rangpurDistrict": [
			{
				"parent_region_id": "rangpurDivision",
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
		],
		"bargunaDistrict": [
			{
				"parent_region_id": "barisalDivision",
				"region_name": "Amtali",
				"region_id": "amtaliUpazila",
				"area": 43,
				"population": 102
			},
			{
				"region_name": "Barguna sadar",
				"region_id": "bargunasadarUpazila",
				"area": 43,
				"population": 102
			},
			{
				"region_name": "Taltali",
				"region_id": "taltaliUpazila",
				"area": 37,
				"population": 101
			}
		]
	};

	var selectedAllDistrict = selectedDivision = selectedDistrict = '';
	var regionsOfBangladesh = regionsOfBangladeshDemo;
	var regionKey = 'bangladesh';
	var title = 'Bangladesh MAP';

	selectedDistrict = 'rangpurDistrict';

	if (selectedAllDistrict == 'allDistrict') {
		regionKey = selectedAllDistrict;
	}

	if (selectedDivision != '') {
		regionKey = selectedDivision;
	}

	if (selectedDistrict != '') {
		regionKey = selectedDistrict;
	}


	$("#changeSvgMap").generateMap({
		'title': title,
		'regionKey': regionKey,
		'regionData': regionsOfBangladesh
	});
});